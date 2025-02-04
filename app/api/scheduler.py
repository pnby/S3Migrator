from datetime import datetime
from typing import final

import schedule
import time

from app.utils.utils import singleton

from app import logger


@final
@singleton
class Scheduler:
    def __init__(self):
        self.jobs = set()
        self.add_interval_task(10, self.print_current_time)

    def add_tasks(self, time_str, task):
        job = schedule.every().day.at(time_str).do(task)
        self.jobs.add(job)

    def add_interval_task(self, interval_seconds, task):
        job = schedule.every(interval_seconds).seconds.do(task)
        self.jobs.add(job)

    @staticmethod
    def print_current_time():
        logger.info(f"Current time: {datetime.now()}")

    @staticmethod
    def run():
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            pass
