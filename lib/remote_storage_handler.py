from datetime import datetime, timezone, timedelta
import logging
import mimetypes
import os
import shutil
import time
import traceback
from stat import S_ISDIR
from contextlib import contextmanager

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from urllib.parse import urljoin, quote

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError, PartialCredentialsError

from paramiko import SSHClient, AutoAddPolicy, RSAKey, SSHException

module_logger = logging.getLogger('icad_tr_consumer.file_storage')


def get_archive_class(archive_config):
    if archive_config.get("archive_type") == 'scp':
        return SCPStorage(archive_config.get('scp'))
    elif archive_config.get("archive_type") == 'google_cloud':
        return GoogleCloudStorage(archive_config.get('google_cloud'))
    elif archive_config.get("archive_type") == 'aws_s3':
        return AWSS3Storage(archive_config.get('aws_s3'))
    elif archive_config.get("archive_type") == 'local':
        return LocalStorage(archive_config.get('local'))
    else:
        module_logger.error('Invalid remote storage type.')
        return None


class GoogleCloudStorage:

    def __init__(self, storage_config):
        try:

            self.storage_client = storage.Client.from_service_account_json(
                storage_config['credentials_file'], project=storage_config['project_id'])
            self.bucket_name = storage_config['bucket_name']
            self.bucket = self.storage_client.get_bucket(self.bucket_name)
        except KeyError as e:
            module_logger.error(f"Google Cloud Missing required configuration data: {e}")
        except GoogleCloudError as e:
            module_logger.error(f"Google Cloud Storage error: {e}")

    def upload_file(self, source_file_path, destination_file_path, destination_generated_path, max_attempts=3):
        try:
            if not os.path.exists(source_file_path) or not os.path.isfile(source_file_path):
                module_logger.error(f'Source file {source_file_path} does not exist or is not a file.')
                return False

            mime_type, _ = mimetypes.guess_type(source_file_path)
            if mime_type is None:
                mime_type = 'application/octet-stream'

            if self.bucket:
                blob = self.bucket.blob(destination_file_path)

                with open(source_file_path, 'rb') as file:
                    blob.upload_from_file(file, content_type=mime_type, timezone=10, retry=2)

                blob.make_public()

                return blob.public_url
            else:
                module_logger.warning("Google Storage Bucket is not available.")
                return None
        except GoogleCloudError as e:
            module_logger.error(f"Failed to upload file to Google Cloud Storage: {e}")
            return None

    def clean_files(self, archive_path, archive_days):
        delete_count = 0
        try:
            now = datetime.now(timezone.utc)

            blobs = self.bucket.list_blobs(prefix=archive_path)

            for blob in blobs:
                age_days = (now - blob.time_created).total_seconds() / 86400  # Convert seconds to days
                if age_days > archive_days:
                    blob.delete()
                    delete_count += 1

            module_logger.info(f"Deleted {delete_count} files from bucket {self.bucket}/{archive_path}")
            return delete_count

        except GoogleCloudError as e:
            module_logger.error(f"Failed to clean Google Cloud Storage: {e}")
            return None


class AWSS3Storage:

    def __init__(self, storage_config):
        try:

            if not storage_config.get("access_key_id", "") or not storage_config.get("secret_access_key", "") or not storage_config.get('bucket_name', ""):
                module_logger.error(f"AWS S3 Missing required configuration data.")
                return

            self.s3 = boto3.resource(
                's3',
                aws_access_key_id=storage_config.get("access_key_id", ""),
                aws_secret_access_key=storage_config.get("secret_access_key", ""),
                connect_timeout=10,
                read_timeout=15,
                retries={
                    'max_attempts': 3,
                    'mode': 'standard'
                }
            )
            self.bucket_name = storage_config.get('bucket_name', "")
            self.bucket = self.s3.Bucket(self.bucket_name)

        except KeyError as e:
            module_logger.error(f"AWS S3 Missing required configuration data: {e}")
        except NoCredentialsError as e:
            module_logger.error(f"Credentials not available for AWS S3: {e}")
        except PartialCredentialsError as e:
            module_logger.error(f"Incomplete credentials provided: {e}")
        except ClientError as e:
            module_logger.error(f"Failed to establish session with AWS S3: {e}")
        except Exception as e:
            module_logger.error(f"An unexpected error occurred: {e}")

    def upload_file(self, source_file_path, destination_file_path, destination_generated_path, max_attempts=3):\

        if not os.path.exists(source_file_path) or not os.path.isfile(source_file_path):
            module_logger.error(f'Source file {source_file_path} does not exist or is not a file.')
            return None

        try:
            with open(source_file_path, 'rb') as file:
                self.bucket.put_object(Key=destination_file_path, Body=file)

            self.s3.ObjectAcl(self.bucket_name, destination_file_path).put(ACL='public-read')

            # Encode the basename of the local_audio_path to ensure it's URL-safe
            encoded_file_name = quote(os.path.basename(destination_file_path))

            # First, join the base URL with the current_date
            url_with_date = urljoin(f'https://{self.bucket_name}.s3.amazonaws.com/',
                                    os.path.dirname(destination_file_path) + '/')

            # Then, join the result with the encoded file name
            return urljoin(url_with_date, encoded_file_name)

        except FileNotFoundError:
            module_logger.error(f"Local file {source_file_path} not found.")
            return None
        except (ClientError, ParamValidationError) as e:
            module_logger.error(f"Error uploading file to AWS S3: {e}")
            return None

    def clean_files(self, archive_path, archive_days):

        s3_client = self.s3.meta.client
        bucket_name = self.bucket_name  # Your S3 bucket name

        delete_count = 0

        try:
            # List all objects within the bucket under the specific prefix
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=archive_path)

            current_time = datetime.now(timezone.utc)
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page['Contents']:
                        last_modified = obj['LastModified']
                        if current_time - last_modified > timedelta(days=archive_days):
                            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                            delete_count += 1

            module_logger.info(f"Deleted {delete_count} files from bucket {bucket_name}/{archive_path}")

            return delete_count

        except ClientError as e:
            module_logger.error(f"Error cleaning S3 files: {e}")
            return None
        except Exception as e:
            module_logger.error(f"Unexpected Error Cleaning S3 Files: {e}")
            return None


class SCPStorage:
    def __init__(self, storage_config):
        self.host = storage_config.get("host")
        self.port = storage_config.get("port", 22)
        self.username = storage_config.get("user", "")
        self.password = storage_config.get("password", "")
        self.private_key_path = storage_config.get('private_key_path', "")
        self.base_url = storage_config.get('base_url', "")

    def ensure_destination_directory_exists(self, sftp, destination_directory):
        """Ensure the remote directory structure exists."""
        parts = destination_directory.split("/")
        current_path = ""

        for part in parts[1:]:
            current_path = f'{current_path}/{part}'.replace("\\", "/")
            try:
                sftp.stat(current_path)
            except FileNotFoundError:
                sftp.mkdir(current_path)
                module_logger.debug(f"Created SCP destination path {current_path}")
            except Exception as e:
                traceback.print_exc()
                module_logger.error(f"SCP Unhandled Exception: {e}")

    def upload_file(self, source_file_path, destination_file_path, destination_generated_path, max_attempts=3):
        """Uploads a file to the SCP storage."""

        if not os.path.exists(source_file_path) or not os.path.isfile(source_file_path):
            module_logger.error(f'Source file {source_file_path} does not exist or is not a file.')
            return False

        for attempt in range(1, max_attempts + 1):
            try:
                module_logger.debug(f"Attempt {attempt} to upload {source_file_path} to {destination_file_path}")

                start_time = time.time()
                with self._create_sftp_session() as (ssh_client, sftp):
                    connection_time = time.time() - start_time
                    module_logger.debug(f"Connected to {self.host} in {connection_time:.2f} seconds.")

                    # Ensure directory exists
                    self.ensure_destination_directory_exists(sftp, os.path.dirname(destination_file_path))

                    # Start file transfer
                    file_transfer_start = time.time()
                    sftp.put(source_file_path, destination_file_path)
                    file_transfer_time = time.time() - file_transfer_start
                    module_logger.debug(f"Transferred {source_file_path} in {file_transfer_time:.2f} seconds.")

                    # Encode the basename of the file
                    encoded_file_name = quote(os.path.basename(destination_file_path))
                    url_with_date = urljoin(self.base_url + '/', destination_generated_path + '/')
                    return urljoin(url_with_date, encoded_file_name)

            except Exception as error:
                traceback.print_exc()
                module_logger.warning(f'Attempt {attempt} failed: {error}')
                if attempt < max_attempts:
                    time.sleep(5)  # Delay between retries

        module_logger.error(f'All {max_attempts} attempts failed to upload {source_file_path}.')
        return False

    def clean_files(self, archive_path, archive_days):
        """Cleans files and empty folders using a shell command on the remote server."""
        find_files_command = f"find {archive_path} -type f -mtime +{archive_days} -exec rm -f {{}} \\;"
        find_folders_command = f"find {archive_path} -type d -empty -exec rmdir {{}} \\;"

        try:
            with self._create_sftp_session() as (ssh_client, _):
                # First, remove files
                module_logger.debug(f"Running file cleanup command: {find_files_command}")
                stdin, stdout, stderr = ssh_client.exec_command(find_files_command)
                err = stderr.read().decode().strip()
                if err:
                    module_logger.error(f"Error during file cleanup: {err}")
                else:
                    module_logger.info("File cleanup completed.")

                # Then, remove empty directories
                module_logger.debug(f"Running folder cleanup command: {find_folders_command}")
                stdin, stdout, stderr = ssh_client.exec_command(find_folders_command)
                err = stderr.read().decode().strip()
                if err:
                    module_logger.error(f"Error during folder cleanup: {err}")
                else:
                    module_logger.info("Folder cleanup completed.")

        except Exception as e:
            module_logger.error(f"Error during remote cleanup: {e}")

    @contextmanager
    def _create_sftp_session(self, timeout=15):
        """Creates and manages an SFTP session using context management."""
        ssh_client = SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        sftp = None

        try:
            ssh_connect_kwargs = {
                "username": self.username,
                "port": self.port,
                "look_for_keys": False,
                "allow_agent": False,
                "timeout": timeout
            }

            if self.private_key_path and os.path.exists(self.private_key_path):
                try:
                    private_key = RSAKey.from_private_key_file(self.private_key_path)
                    ssh_connect_kwargs["pkey"] = private_key
                except SSHException as e:
                    module_logger.error(f"Failed to load private key: {e}")
                    if self.password:
                        ssh_connect_kwargs["password"] = self.password

            elif self.password:
                ssh_connect_kwargs["password"] = self.password
            else:
                raise ValueError("No valid authentication method provided.")

            start_ssh_time = time.time()
            ssh_client.connect(self.host, **ssh_connect_kwargs)
            ssh_connect_duration = time.time() - start_ssh_time
            module_logger.debug(f"SSH connection to {self.host} established in {ssh_connect_duration:.2f} seconds.")

            sftp = ssh_client.open_sftp()
            yield ssh_client, sftp

        except SSHException as e:
            module_logger.error(f'SSH connection error: {e}')
            raise
        finally:
            if sftp:
                sftp.close()
                module_logger.debug("SFTP session closed.")
            ssh_client.close()
            module_logger.debug("SSH connection closed.")


class LocalStorage:
    def __init__(self, storage_config):
        self.base_url = storage_config.get("base_url", "")

    def ensure_destination_directory_exists(self, destination_directory):
        """Ensure the local directory structure exists."""
        if not os.path.exists(destination_directory):
            os.makedirs(destination_directory)

    def upload_file(self, source_file_path, destination_file_path, destination_generated_path, max_attempts=None):
        """Copies a file to the local storage with a date-based directory structure."""
        if not os.path.exists(source_file_path) or not os.path.isfile(source_file_path):
            module_logger.error(f'Source file {source_file_path} does not exist or is not a file.')
            return False

        try:
            self.ensure_destination_directory_exists(os.path.dirname(destination_file_path))

            shutil.copy(source_file_path, destination_file_path)

            # Encode the basename of the local_audio_path to ensure it's URL-safe
            encoded_file_name = quote(os.path.basename(destination_file_path))

            # First, join the base URL with the current_date
            url_with_date = urljoin(self.base_url + '/', destination_generated_path + '/')

            # Then, join the result with the encoded file name
            return urljoin(url_with_date, encoded_file_name)

        except Exception as error:  # Preferably catch more specific exceptions
            module_logger.warning(f'Local Archive Failed: {error}')
            return False

    def clean_files(self, archive_path, archive_days):
        """Removes files older than a specified number of days within the local archive path."""
        archive_seconds = archive_days * 24 * 3600
        current_time = time.time()

        for root, dirs, files in os.walk(archive_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                if current_time - os.path.getmtime(file_path) >= archive_seconds:
                    os.remove(file_path)
                    module_logger.debug(f"Successfully cleaned local file: {file_path}")

            for name in dirs:
                dir_path = os.path.join(root, name)
                try:
                    os.rmdir(dir_path)  # Try to remove the directory if it's empty
                except OSError:
                    pass  # Directory not empty or other error
