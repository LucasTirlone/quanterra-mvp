import datetime
import logging
import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class S3CsvService:
    def __init__(
        self,
        bucket_name: str,
        prefix: str = "",
        s3_client: Optional[boto3.client] = None,
    ):
        """
        Service for:
            1. Read CSVs from a local folder.
            2. Upload these CSVs to an S3 bucket.
            3. Clean the local folder (all files, including non-CSV).

        :param bucket_name: Destination S3 bucket name.
        :param prefix: Prefix (path) inside the bucket, e.g., "my/system/inputs/".
                                             If empty, uploads to the root of the bucket.
        :param s3_client: Pre-configured boto3 client. If None, creates with default config.
        """
        self.bucket_name = bucket_name
        # Normalize prefix (if not empty, ensure it ends with "/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self.prefix = prefix
        self.s3 = s3_client or boto3.client("s3")

    def _build_s3_key(self, base_folder: Path, file_path: Path) -> str:
        """
        Generates the S3 key preserving the relative structure of the local folder.
        Example:
            base_folder = /data/input
            file_path   = /data/input/subfolder/file.csv
            prefix      = "my-files/"
            => key      = "my-files/subfolder/file.csv"
        """
        rel_path = file_path.relative_to(base_folder)        # subfolder/file.csv
        rel_str = rel_path.as_posix()                        # standardize "/" instead of "\\"
        return f"{self.prefix}{rel_str}" if self.prefix else rel_str

    def upload_csv(
        self,
        local_folder: str | Path,
        file_name: str,
        bucket_name: str,
        s3_folder: str,
    ) -> bool:
        """
        Upload a single CSV file to S3.

        :param local_folder: Local folder path where the file is located.
        :param file_name: Name of the CSV file to upload.
        :param bucket_name: Destination S3 bucket name.
        :param s3_folder: Destination folder path in S3 (e.g., "my/folder/").
        :return: True if upload successful, False otherwise.
        """
        local_folder = Path(local_folder).resolve()
        file_path = local_folder / file_name

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return False

        if not file_path.suffix.lower() == ".csv":
            logger.warning(f"File is not a CSV: {file_path}")
            return False

        # Normalize s3_folder (ensure it ends with "/")
        s3_folder = s3_folder.strip("/")
        if s3_folder and not s3_folder.endswith("/"):
            s3_folder += "/"

        # Build S3 key
        s3_key = f"{s3_folder}{file_name}" if s3_folder else file_name

        logger.info(f"Uploading CSV: {file_path} -> s3://{bucket_name}/{s3_key}")

        try:
            self.s3.upload_file(
                Filename=str(file_path),
                Bucket=bucket_name,
                Key=s3_key,
            )
            logger.info(f"✓ Successfully uploaded: {file_name}")
            return True
        except (BotoCoreError, ClientError) as e:
            logger.error(f"✗ Error uploading {file_name}: {e}")
            return False

    
    def upload_csvs_and_clean(
        self,
        local_folder: str | Path,
        s3_folder: str = "",
        recursive: bool = True,
        dry_run: bool = False,
        bucket_name: str = None,
    ) -> None:
        """
        1. Finds all .csv files in the local folder.
        2. Uploads them to S3.
        3. Removes EVERYTHING inside the folder (files and subfolders).

        :param local_folder: Local folder path (e.g., "C:/data/input" or "/var/data").
        :param s3_folder: Specific folder inside S3 bucket for uploads (e.g., "my/specific/folder/").
                         If empty, uses the prefix defined in __init__.
        :param recursive: If True, also traverses subfolders.
        :param dry_run: If True, only logs what would be done, without actual upload/removal.
        :param bucket_name: Specific bucket to use. If None, uses the bucket defined in __init__.
        """
        # Use provided bucket or default to self.bucket_name
        target_bucket = bucket_name or self.bucket_name
        base_folder = Path(local_folder).resolve()

        if not base_folder.exists() or not base_folder.is_dir():
            raise ValueError(f"Invalid local folder: {base_folder}")

        logger.info(f"Processing local folder: {base_folder}")
        logger.info(f"Target bucket: s3://{target_bucket}")

        # 1. Find CSVs
        if recursive:
            csv_files = list(base_folder.rglob("*.csv"))
        else:
            csv_files = list(base_folder.glob("*.csv"))

        if not csv_files:
            logger.info("No CSV files found in the folder.")
        else:
            logger.info(f"Found {len(csv_files)} CSV files to upload.")

        # 2. Upload to S3
        # Use s3_folder if provided, otherwise use prefix
        s3_base = s3_folder if s3_folder else self.prefix
        # Normalize s3_base (if not empty, ensure it ends with "/")
        if s3_base and not s3_base.endswith("/"):
            s3_base += "/"
        
        for file_path in csv_files:
            # Use only the filename (no relative path from local folder)
            filename = file_path.name
            s3_key = f"{s3_base}{filename}" if s3_base else filename
            logger.info(f"Upload CSV: {file_path} -> s3://{target_bucket}/{s3_key}")

            if dry_run:
                continue

            try:
                self.s3.upload_file(
                    Filename=str(file_path),
                    Bucket=target_bucket,
                    Key=s3_key,
                )
            except (BotoCoreError, ClientError) as e:
                # Here you decide: fail everything or just log and continue to the next
                logger.info(f"Error uploading {file_path}: {e}")
                # If you want to abort everything on error, you can `raise` here.
                # raise

        # 3. Clean the local folder (all files, including non-CSV)
        logger.info(f"Cleaning contents of: {base_folder}")

        if dry_run:
            logger.info("[DRY-RUN] No files will be deleted.")
            return

        # Traverse the tree from bottom to top to be able to remove empty subfolders
        for root, dirs, files in os.walk(base_folder, topdown=False):
            root_path = Path(root)

            # Remove files
            for f in files:
                file_to_remove = root_path / f
                try:
                    file_to_remove.unlink()
                    logger.info(f"Removed file: {file_to_remove}")
                except OSError as e:
                    logger.info(f"Error removing file {file_to_remove}: {e}")

            # Remove subfolders (but never remove the base_root folder)
            if root_path != base_folder:
                try:
                    root_path.rmdir()
                    logger.info(f"Removed folder: {root_path}")
                except OSError as e:
                    # Folder not empty or other error
                    logger.info(f"Error removing folder {root_path}: {e}")

        logger.info("Cleanup completed. The folder is empty (only the root remains).")

    def move_files(
        self,
        file_list: list[str],
        source_folder: str,
        destination_folder: str,
        dry_run: bool = False,
        bucket_name: str = None,
    ) -> dict:
        """
        Move files from source folder to destination folder in S3.

        :param file_list: List of file names to move (e.g., ["file1.csv", "file2.csv"]).
        :param source_folder: Source folder path in S3 (e.g., "my/source/folder").
                              Can optionally end with "/" or not.
        :param destination_folder: Destination folder path in S3 (e.g., "my/dest/folder").
                                   Can optionally end with "/" or not.
        :param dry_run: If True, only logs what would be done without actual operations.
        :param bucket_name: Specific bucket to use. If None, uses the bucket defined in __init__.
        :return: Dictionary with operation results containing:
                 - "moved": list of successfully moved files
                 - "failed": list of files that failed to move
                 - "total": total number of files attempted
        """
        # Use provided bucket or default to self.bucket_name
        target_bucket = bucket_name or self.bucket_name
        # Normalize folders (ensure no leading "/" but add trailing "/" if needed)
        source_folder = source_folder.strip("/")
        destination_folder = destination_folder.strip("/")
        
        if not source_folder:
            raise ValueError("source_folder cannot be empty")
        if not destination_folder:
            raise ValueError("destination_folder cannot be empty")
        
        if source_folder == destination_folder:
            raise ValueError("source_folder and destination_folder must be different")

        moved_files = []
        failed_files = []
        
        logger.info(f"Starting S3 file move operation:")
        logger.info(f"  Bucket: s3://{target_bucket}")
        logger.info(f"  Source: s3://{target_bucket}/{source_folder}/")
        logger.info(f"  Destination: s3://{target_bucket}/{destination_folder}/")
        logger.info(f"  Files to move: {len(file_list)}")
        
        if dry_run:
            logger.info("[DRY-RUN MODE]")

        for file_name in file_list:
            # Build source and destination keys
            source_key = f"{source_folder}/{file_name}"
            destination_key = f"{destination_folder}/{file_name}_{(datetime.now().strftime('%m%d%Y'))}"
            
            try:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would move: s3://{target_bucket}/{source_key} -> s3://{target_bucket}/{destination_key}")
                    moved_files.append(file_name)
                else:
                    # Copy file from source to destination
                    logger.info(f"Copying: s3://{target_bucket}/{source_key} -> s3://{target_bucket}/{destination_key}")
                    copy_source = {
                        'Bucket': target_bucket,
                        'Key': source_key
                    }
                    self.s3.copy_object(
                        CopySource=copy_source,
                        Bucket=target_bucket,
                        Key=destination_key,
                    )
                    
                    # Delete file from source
                    logger.info(f"Deleting source: s3://{target_bucket}/{source_key}")
                    self.s3.delete_object(
                        Bucket=target_bucket,
                        Key=source_key,
                    )
                    
                    moved_files.append(file_name)
                    logger.info(f"✓ Successfully moved: {file_name}")
                    
            except (BotoCoreError, ClientError) as e:
                failed_files.append(file_name)
                logger.info(f"✗ Error moving {file_name}: {e}")

        # Log summary
        logger.info(f"\n--- Move Operation Summary ---")
        logger.info(f"Total files: {len(file_list)}")
        logger.info(f"Successfully moved: {len(moved_files)}")
        logger.info(f"Failed: {len(failed_files)}")
        
        if failed_files:
            logger.info(f"Failed files: {failed_files}")

        return {
            "moved": moved_files,
            "failed": failed_files,
            "total": len(file_list),
        }

    def list_csv_files(
        self,
        folder: str,
        recursive: bool = True,
        bucket_name: str = None,
    ) -> list[str]:
        """
        List all CSV files in a specific S3 folder.

        :param folder: Folder path in S3 (e.g., "my/folder" or "my/folder/").
                      Can optionally end with "/" or not.
        :param recursive: If True, lists CSV files in subfolders as well.
                         If False, only lists files directly in the folder (not in subfolders).
        :param bucket_name: Specific bucket to use. If None, uses the bucket defined in __init__.
        :return: List of file keys (full paths from S3 root) for all CSV files found.
        """
        # Use provided bucket or default to self.bucket_name
        target_bucket = bucket_name or self.bucket_name
        # Normalize folder (remove leading "/" and ensure trailing "/" for prefix search)
        folder = folder.strip("/")
        
        if not folder:
            raise ValueError("folder cannot be empty")
        
        # Add trailing slash for prefix search
        prefix = f"{folder}/"
        
        csv_files = []
        
        logger.info(f"Listing CSV files from s3://{target_bucket}/{prefix}")
        
        try:
            # Use paginator to handle folders with many files
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=target_bucket, Prefix=prefix)
            
            for page in pages:
                # Check if the page contains any contents
                if "Contents" not in page:
                    continue
                
                for obj in page["Contents"]:
                    key = obj["Key"]
                    
                    # Skip the folder itself (if it exists as an object)
                    if key.endswith("/"):
                        continue
                    
                    # Check if it's a CSV file
                    if key.lower().endswith(".csv"):
                        # If not recursive, only include files directly in the folder (no subfolders)
                        if not recursive:
                            # Count "/" after the prefix to determine if it's in a subfolder
                            relative_path = key[len(prefix):]
                            if "/" in relative_path:
                                # It's in a subfolder, skip it
                                continue
                        
                        csv_files.append(key)
                        logger.info(f"Found CSV file: {key}")
            
            logger.info(f"Total CSV files found: {len(csv_files)}")
            
        except ClientError as e:
            logger.error(f"Error listing files from S3: {e}")
            raise
        
        return csv_files


    def download_csv_file(
        self,
        s3_key: str,
        local_path: str | Path = None,
        bucket_name: str = None,
    ) -> bytes | str:
        """
        Download a CSV file from S3 by its key.

        :param s3_key: Full S3 key/path of the file (e.g., "processed/file1.csv").
        :param local_path: Optional local path to save the file. If None, returns file content as bytes.
                          If provided, saves the file locally and returns the local path.
        :param bucket_name: Specific bucket to use. If None, uses the bucket defined in __init__.
        :return: If local_path is None, returns file content as bytes.
                If local_path is provided, returns the local file path as string.
        """
        # Use provided bucket or default to self.bucket_name
        target_bucket = bucket_name or self.bucket_name
        
        if not s3_key:
            raise ValueError("s3_key cannot be empty")
        
        logger.info(f"Downloading CSV file from s3://{target_bucket}/{s3_key}")
        
        try:
            if local_path:
                # Save to local file
                local_path = Path(local_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                self.s3.download_file(
                    Bucket=target_bucket,
                    Key=s3_key,
                    Filename=str(local_path),
                )
                
                logger.info(f"File saved to: {local_path}")
                return str(local_path)
            else:
                # Return file content as bytes
                response = self.s3.get_object(
                    Bucket=target_bucket,
                    Key=s3_key,
                )
                
                file_content = response["Body"].read()
                logger.info(f"File downloaded to memory ({len(file_content)} bytes)")
                return file_content
                
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise


    def clean_local_files(
        self,
        local_folder: str | Path,
        file_list: list[str],
        dry_run: bool = False,
    ) -> dict:
        """
        Delete specific files from a local folder.

        :param local_folder: Local folder path (e.g., "C:/data/input" or "/var/data").
        :param file_list: List of file names or relative paths to delete (e.g., ["file1.csv", "subfolder/file2.csv"]).
        :param dry_run: If True, only logs what would be done without actual deletion.
        :return: Dictionary with operation results containing:
                 - "deleted": list of successfully deleted files
                 - "failed": list of files that failed to delete (not found, permission issues, etc.)
                 - "total": total number of files attempted
        """
        local_folder = Path(local_folder).resolve()
        
        if not local_folder.exists() or not local_folder.is_dir():
            raise ValueError(f"Invalid local folder: {local_folder}")
        
        deleted_files = []
        failed_files = []
        
        logger.info(f"Starting cleanup of local folder: {local_folder}")
        logger.info(f"Files to delete: {len(file_list)}")
        
        if dry_run:
            logger.info("[DRY-RUN MODE]")
        
        for file_name in file_list:
            file_path = local_folder / file_name
            
            try:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would delete: {file_path}")
                    deleted_files.append(file_name)
                else:
                    if not file_path.exists():
                        logger.warning(f"File not found: {file_path}")
                        failed_files.append(file_name)
                        continue
                    
                    file_path.unlink()
                    deleted_files.append(file_name)
                    logger.info(f"✓ Deleted: {file_path}")
                    
            except (OSError, PermissionError) as e:
                failed_files.append(file_name)
                logger.error(f"✗ Error deleting {file_path}: {e}")
        
        # Log summary
        logger.info(f"\n--- Cleanup Summary ---")
        logger.info(f"Total files: {len(file_list)}")
        logger.info(f"Successfully deleted: {len(deleted_files)}")
        logger.info(f"Failed: {len(failed_files)}")
        
        if failed_files:
            logger.info(f"Failed deletions: {failed_files}")
        
        return {
            "deleted": deleted_files,
            "failed": failed_files,
            "total": len(file_list),
        }

