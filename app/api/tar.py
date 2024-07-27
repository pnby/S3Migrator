import tarfile
import os
import time
from typing import LiteralString, cast, Optional, final

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

    def _create_tar(self) -> None:
        try:
            self.tar_path = cast(str, os.path.join(self.destination_dir, self.tar_name + ".tar"))
            if os.path.exists(self.tar_path):
                logger.info(f"{self.tar_path} already exists. Skipping creation.")
                return

            with tarfile.open(self.tar_path, "w") as tar:
                for root, dirs, files in os.walk(self.source_dir):
                    for file in files:
                        file_path = cast(LiteralString, os.path.join(root, file))
                        arcname = os.path.relpath(file_path, start=self.source_dir)
                        tar.add(file_path, arcname=arcname)
                        logger.info(f"Added {arcname} to {self.tar_path}")
        except Exception as e:
            logger.error(e)

    def create_tar(self) -> Optional[str]:
        start_time = time.time()
        self._create_tar()
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"File copying is finished in {total_time} seconds")
        logger.info(self.tar_path)
        return self.tar_path
