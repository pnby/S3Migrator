import os
from datetime import datetime, timezone, timedelta
from typing import final, Optional, List

import boto3

from app import logger
from app.api.scheduler import Scheduler
from app.api.tar import TarManager
from app.utils.config import SECRET_KEY, ACCESS_KEY_ID
from app.utils.utils import singleton


@final
@singleton
class BackupManager:
    _scheduler: Scheduler = Scheduler()
    _bucket: str = "backups"

    def __init__(self, source_dir: str, dest_dir: str, timestamp: str, extra_dirs: Optional[List[str]] = None):
        self._filename = f"backup-{timestamp}"
        self._tar_manager = TarManager(
            source_dir=source_dir,
            destination_dir=dest_dir,
            tar_name=self._filename,
            extra_dirs=extra_dirs
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
        tar_path = self._tar_manager.create_tar()
        if tar_path is None:
            logger.error("TarPath is None, backup procedure failed")
            return

        logger.info("Starting file uploading")
        self._client.upload_file(tar_path, self._bucket, self._filename + ".tar")
        logger.info("File uploading successfully ended, removing redundant archives")
        try:
            os.remove(tar_path)
            logger.info(f"Removed local archive file: {tar_path}")
            self.cleanup_old_backups()
        except Exception as e:
            logger.error(f"Error removing local archive file: {e}")

    def cleanup_old_backups(self, keep_count: int = 5):
        logger.info(f"Cleaning up old backups, keeping the latest {keep_count} backups")

        response = self._client.list_objects_v2(Bucket=self._bucket)
        if 'Contents' not in response:
            logger.info("No backups found to clean up.")
            return

        backups: List[dict] = response['Contents']

        backups.sort(key=lambda obj: obj['LastModified'], reverse=True)

        for obj in backups[keep_count:]:
            logger.info(f"Deleting old backup: {obj['Key']}")
            self._client.delete_object(Bucket=self._bucket, Key=obj['Key'])

    def start_upload_task(self, time: str):
        self._scheduler.add_tasks(time, self.upload_backup)
        self._scheduler.run()