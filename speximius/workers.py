import argparse

from circleup.utils.cloud import sh_connection
from circleup.utils.project import (
    get_project_settings, get_logger, DEV_PROJECT_ID
)


settings, project_conf = get_project_settings()
logging = get_logger(__name__)


class ParallelWorker(object):

    def __init__(self):
        self.args = self.parse_args()
        self.logger = logging
        self.apikey = self.args.apikey

    def __enter__(self):
        self.sh_conn = sh_connection(
            self.args.project
            or project_conf.get('project_id')
            or DEV_PROJECT_ID,
            api_key=self.apikey
        )
        self.project_key = self.sh_conn.project_key
        return self

    def __exit__(self, typ, val, tracebck):
        self.sh_conn.sh_client.close()

    def get_job(self, job_id):
        return self.sh_conn.sh_client.get_job(job_id)

    def get_logs(self, job_id=None, job=None, count=100):
        if job_id and not job:
            job = self.get_job(job_id)
        elif not job_id and not job:
            self.logger.error('You must provide a job_id or an actual job')
            yield ''
            return

        lines = 0
        for line in job.logs.iter(count=count, offset=0):
            yield line
            lines += 1
            if lines > count:
                break

    def process_job(self, job_id):
        ''' What to do with each individual job will depend
        on specific implementations.

        At this point any class that might subclass has available:
         - scrapinghub client: self.sh_conn.sh_client
         - project: self.sh_conn.sh_project
         - project_id: self.sh_conn.project_id

        The right apikey will also have been used to create the sh client
        '''
        raise NotImplementedError(
            'You must subclass and implement the process_job method'
        )

    def run(self):
        try:
            for job_id in self.args.job:
                self.process_job(job_id)

        except Exception as e:
            self.logger.exception(e)

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--job', help='job id to process', action='append')
        parser.add_argument('--project', help='project id')
        parser.add_argument(
            '--apikey', help='scrapinghub api key to use',
            default=settings['SCRAPINGHUB_APIKEY']
        )
        return parser.parse_args()
