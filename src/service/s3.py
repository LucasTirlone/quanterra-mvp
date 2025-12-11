import os
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError


class LocalFolderToS3CsvService:
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

    def upload_csvs_and_clean(
        self,
        local_folder: str | Path,
        s3_folder: str = "",
        recursive: bool = True,
        dry_run: bool = False,
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
        """
        base_folder = Path(local_folder).resolve()

        if not base_folder.exists() or not base_folder.is_dir():
            raise ValueError(f"Invalid local folder: {base_folder}")

        print(f"Processing local folder: {base_folder}")

        # 1. Find CSVs
        if recursive:
            csv_files = list(base_folder.rglob("*.csv"))
        else:
            csv_files = list(base_folder.glob("*.csv"))

        if not csv_files:
            print("No CSV files found in the folder.")
        else:
            print(f"Found {len(csv_files)} CSV files to upload.")

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
            print(f"Upload CSV: {file_path} -> s3://{self.bucket_name}/{s3_key}")

            if dry_run:
                continue

            try:
                self.s3.upload_file(
                    Filename=str(file_path),
                    Bucket=self.bucket_name,
                    Key=s3_key,
                )
            except (BotoCoreError, ClientError) as e:
                # Here you decide: fail everything or just log and continue to the next
                print(f"Error uploading {file_path}: {e}")
                # If you want to abort everything on error, you can `raise` here.
                # raise

        # 3. Clean the local folder (all files, including non-CSV)
        print(f"Cleaning contents of: {base_folder}")

        if dry_run:
            print("[DRY-RUN] No files will be deleted.")
            return

        # Traverse the tree from bottom to top to be able to remove empty subfolders
        for root, dirs, files in os.walk(base_folder, topdown=False):
            root_path = Path(root)

            # Remove files
            for f in files:
                file_to_remove = root_path / f
                try:
                    file_to_remove.unlink()
                    print(f"Removed file: {file_to_remove}")
                except OSError as e:
                    print(f"Error removing file {file_to_remove}: {e}")

            # Remove subfolders (but never remove the base_root folder)
            if root_path != base_folder:
                try:
                    root_path.rmdir()
                    print(f"Removed folder: {root_path}")
                except OSError as e:
                    # Folder not empty or other error
                    print(f"Error removing folder {root_path}: {e}")

        print("Cleanup completed. The folder is empty (only the root remains).")
