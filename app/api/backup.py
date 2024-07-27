import os
import subprocess
from datetime import datetime, timezone, timedelta
from typing import final

import boto3

from app import logger
from app.api.scheduler import Scheduler
from app.api.tar import TarManager
from app.utils.config import SECRET_KEY, ACCESS_KEY_ID
from app.utils.utils import singleton


@final
@singleton
class BackupManager:
    _tar_manager: TarManager
    _scheduler: Scheduler = Scheduler()
    _bucket: str = "backups"

    def __init__(self, source_dir: str, dest_dir: str, timestamp: str):
        self._filename = f"backup-{timestamp}"
        self._tar_manager = TarManager(
            source_dir=source_dir,
            destination_dir=dest_dir,
            tar_name=self._tar_manager
        )
        self._client = boto3.client("s3",
                                    endpoint_url="https://statew.s3.ru-1.storage.selcloud.ru",
                                    region_name="ru-1",
                                    aws_access_key_id=ACCESS_KEY_ID,
                                    aws_secret_access_key=SECRET_KEY,
                                    verify=False
                                    )

    def upload_backup(self):
        logger.info("Starting backup procedure")
        self.clear_old_files()
        tar_path = self._tar_manager.create_tar()
        if tar_path is None:
            logger.error("TarPath is None, backup procedure failed")
            return

        logger.info("Starting file uploading")
        self._client.upload_file(tar_path, self._bucket, tar_path)
        logger.info("File uploading successfully ended, removing redundant archives")
        try:
            os.remove(tar_path)
            logger.info(f"Removed local archive file: {tar_path}")
        except Exception as e:
            logger.error(f"Error removing local archive file: {e}")

    def start_upload_task(self, time: str):
        self._scheduler.add_tasks(time, self.upload_backup)
        self._scheduler.run()

    def clear_old_files(self, days: int = 2, application: str = "x-tar"):
        logger.info(f"Starting cleanup of files older than {days} days")
        now = datetime.now(timezone.utc)
        cutoff_date = now - timedelta(days=days)

        response = self._client.list_objects_v2(Bucket=self._bucket)
        if 'Contents' not in response:
            logger.info("No files found in bucket")
            return

        for obj in response['Contents']:
            key = obj['Key']
            last_modified = obj['LastModified']

            if last_modified < cutoff_date and key.endswith(f".{application}"):
                self._client.delete_object(Bucket=self._bucket, Key=key)
                logger.info(f"Deleted file: {key}, last modified at {last_modified}")

    @staticmethod
    def create_mysql_dump(backup_dir: str, user: str, password: str):
        backup_file = os.path.join(backup_dir, 'backup.sql')
        command = f"mysqldump -u {user} -p'{password}' --all-databases > {backup_dir}"

        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"Backup created: {backup_file}")
            else:
                logger.error(f"Error creating backup: {result.stderr}")
                return None
        except subprocess.CalledProcessError as e:
            logger.error(f"Subprocess error: {e}")
            return None

        return backup_file
