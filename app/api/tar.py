import subprocess
import tarfile
import os
import time
from typing import Optional, final, cast

from app.utils.config import MYSQL_USER, MYSQL_PASSWD
from app.utils.utils import singleton
from app import logger


@final
@singleton
class TarManager:
    def __init__(self, source_dir, destination_dir, tar_name):
        self.tar_path: Optional[str] = None
        self.source_dir = source_dir
        self.destination_dir = destination_dir
        self.tar_name = tar_name
        self.dump_path = "/mysql_dumps"

    def _create_tar(self) -> None:
        try:
            self.tar_path = os.path.join(self.destination_dir, self.tar_name + ".tar")
            if os.path.exists(self.tar_path):
                logger.info(f"{self.tar_path} already exists. Skipping creation.")
                return

            with tarfile.open(self.tar_path, "w") as tar:
                for root, _, files in os.walk(self.source_dir):
                    for file in files:
                        file_path = cast(str, os.path.join(root, file))
                        arcname = os.path.relpath(file_path, start=self.source_dir)
                        tar.add(file_path, arcname=arcname)
                        logger.info(f"Added {arcname} to {self.tar_path}")

                dump_path = self._create_mysql_dump()
                if dump_path:
                    arcname = os.path.relpath(dump_path, start=self.dump_path)
                    tar.add(dump_path, arcname=arcname)
                    os.remove(dump_path)
                else:
                    logger.error("Error while creating mysql dump")
        except Exception as e:
            logger.error(e)

    def create_tar(self) -> Optional[str]:
        start_time = time.time()
        self._create_tar()
        total_time = time.time() - start_time
        logger.info(f"File copying is finished in {total_time} seconds")
        logger.info(self.tar_path)
        return self.tar_path

    def _create_mysql_dump(self) -> Optional[str]:
        os.makedirs(self.dump_path, exist_ok=True)
        backup_file = os.path.join(self.dump_path, 'backup.sql')
        command = f"mysqldump -u {MYSQL_USER} -p'{MYSQL_PASSWD}' --all-databases > {backup_file}"

        try:
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                logger.info(f"MySQL backup created: {backup_file}")
                return backup_file
            else:
                logger.error(f"Error creating MySQL backup: {result.stderr}")
                return None
        except subprocess.CalledProcessError as e:
            logger.error(f"Subprocess error: {e}")
            return None