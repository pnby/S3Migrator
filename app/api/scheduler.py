from typing import final

import schedule
import time

from app.utils.utils import singleton


@final
@singleton
class Scheduler:
    def __init__(self):
        self.jobs = set()

    def add_tasks(self, time_str, task):
        job = schedule.every().day.at(time_str).do(task)
        self.jobs.add(job)

    @staticmethod
    def run():
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            pass
