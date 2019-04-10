import pkg_resources
import json
import argparse

from md5 import md5
from json import loads, dumps
from uuid import uuid4
from time import sleep
from itertools import cycle

from circleup.settings import UTILITY_APIKEYS
from circleup.utils.cloud import (
    sh_connection, get_jobs, get_job_items, post_to_collection,
    get_collection_item
)
from circleup.utils.project import (
    get_project_settings, get_logger, DEV_PROJECT_ID
)


settings, project_conf = get_project_settings()
logging = get_logger(__name__)

APIKEYS = UTILITY_APIKEYS.values()
MAX_RETRIES = 2
DEFAULT_JOBS_PER_WORKER = 1
DEFAULT_WORKERS = 8
SLEEP_BETWEEN_CHECKS = 10
COLLS_API_URL = (
    'https://storage.scrapinghub.com/collections/%(project_key)s'
)


class ParallelJobsDispatcher(object):

    def __init__(self, spider=None,  worker_script=None, test=False):
        self.worker_script = worker_script
        self.args = self.parse_args()
        spider = spider or self.args.spider
        if not spider:
            raise ValueError(
                'spider must be specified either as cmd arg or in constructor'
            )
        self.spider_name = spider
        self.test_mode = test
        self.logger = logging
        self.apikey_cycler = cycle(APIKEYS)

    def __enter__(self):
        self.sh_conn = sh_connection(
            self.args.project
            or project_conf.get('project_id')
            or DEV_PROJECT_ID
        )
        return self

    def __exit__(self, typ, val, tracebck):
        self.sh_conn.sh_client.close()

    @property
    def apikey(self):
        return next(self.apikey_cycler)

    def get_jobs_processed(self, sh_project):
        return get_jobs(
            sh_project, spider_name=self.spider_name,
            lacks_tag=['delivered'],
            status=['finished']
        )

    def start_worker(self, project_key, sh_project,
                     process_id, jobs_to_process):
        if not jobs_to_process:
            self.logger.info('No job to process, nothing to do.')
            return

        job_ids = ' '.join(
            '--job=%s' % job_id for job_id in jobs_to_process
        )
        job_args = '--project=%s --apikey=%s %s' % (
            project_key, self.apikey, job_ids
        )

        if not self.test_mode:
            sh_project.jobs.run(
                spider=self.worker_script, cmd_args=job_args,
                add_tag=process_id,
            )
        for job in jobs_to_process:
            self.logger.info('Started new job for %s' % str(job))

    def get_active_workers_num(self, sh_project):
        return sum(
            1 for job in get_jobs(
                sh_project, spider_name=self.worker_script,
                status=['running', 'pending']
            )
        )

    def run(self, project_id=None, workers=DEFAULT_WORKERS,
            jobs_per_worker=DEFAULT_JOBS_PER_WORKER):

        workers = self.args.workers or workers
        jobs_per_worker = self.args.jobs_per_worker or jobs_per_worker
        project_id = self.args.project or project_id

        process_id = 'pid_%s' % uuid4().hex
        jobs_to_process, total_jobs = [], 0
        jobs = self.get_jobs_processed(self.sh_conn.sh_project)

        try:
            job_data = next(jobs)
        except StopIteration:
            return

        while True:
            jobs_to_process.append(job_data)
            if len(jobs_to_process) == jobs_per_worker:
                total_jobs += 1

                job_was_started = self.start_worker(
                    self.sh_conn.project_key, self.sh_conn.sh_project,
                    process_id, jobs_to_process
                )
                jobs_to_process = []

                # give couple of seconds for the job to go to
                # pending / running state, otherwise we might end
                # queuing hundreds of jobs
                if job_was_started:
                    sleep(5)

                while True:
                    active_workers = self.get_active_workers_num(
                        self.sh_conn.sh_project
                    )
                    self.logger.info('Workers %d' % active_workers)

                    if active_workers >= workers:
                        sleep(SLEEP_BETWEEN_CHECKS)
                    else:
                        break
            try:
                job_data = next(jobs)
            except StopIteration:
                break

        # left overs from jobs_to_process if last iter did not
        # reach JOBS_PER_WORKER
        if jobs_to_process:
            self.start_worker(
                self.sh_conn.project_key, self.sh_conn.sh_project,
                process_id, jobs_to_process
            )

    def parse_args(self):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            '--workers', type=int, default=DEFAULT_WORKERS,
            help='Number of paralel workers doing the processing'
        )
        parser.add_argument(
            '--jobs-per-worker', type=int, default=10,
            help='Number of jobs to be process per worker'
        )
        parser.add_argument(
            '--spider', help='Spider name'
        )

        parser.add_argument('--project')
        parser.add_argument('--test-mode')

        return parser.parse_args()


class ParallelItemDispatcher(ParallelJobsDispatcher):

    def __enter__(self):
        self.project_id = (
            self.args.project
            or project_conf.get('project_id')
            or DEV_PROJECT_ID
        )
        self.sh_conn = sh_connection(self.project_id)
        if self.args.begin:
            self.delete_job_tracker_collection()

        return self

    @staticmethod
    def generate_key(item):
        return md5(item.encode('utf-8')).hexdigest()

    def get_active_workers_num(self, sh_project):
        return sum(
            1 for job in get_jobs(
                sh_project, spider_name=self.args.consumer,
                status=['running', 'pending']
            )
        )

    def get_jobs_processed(self, sh_project):
        return get_job_items(
            self.sh_conn.sh_client, self.args.consume_job_items
        )

    def get_tracker_collection_url(self):
        return COLLS_API_URL % (dict(project_key=self.project_id))

    def delete_job_tracker_collection(self):
        url = '%s/delete?name=jobs_%s' % (
            self.get_tracker_collection_url(), self.spider_name
        )
        post_to_collection(url, None)

    def add_job_to_tracker_collection(self, item_id):
        payload = dumps({
            '_key': item_id, 'success': False, 'item_id': item_id
        })
        url = '%s/s/jobs_%s' % (
            self.get_tracker_collection_url(), self.spider_name
        )
        post_to_collection(url, payload)

    def get_previous_run(self, item):
        url = '%s/s/jobs_%s' % (
            self.get_tracker_collection_url(), self.spider_name
        )
        key = self.generate_key(item)
        return (
            get_collection_item(url, key, logger=self.logger)
            or
            {'success': False, 'item_id': 'UNKNOWN', 'retries': 0}
        )

    def should_rerun_job(self, process_id, previous_run):
        if not previous_run:
            self.logger.info(
                'No previous run detected, running job'
            )
            return True

        item = previous_run.get('item_id')
        if previous_run.get('success'):
            self.logger.info(
                'Job already resolved succesfully for item %s.' % item
            )
            return False

        elif int(previous_run.get('retries', 0)) > MAX_RETRIES:
            self.logger.info(
                'Job already retried more than once.'
                ' No more retries for job for item %s' % item
            )
            return False

        elif str(item) in self.get_active_workers_items_in_use():
            self.logger.info(
                'No previous succesful job found for %s' % item
            )
            return False

        return True

    def resources(self, process_id):
        return None

    def get_next_job_resource(self, process_id):
        resources = self.resources(process_id)
        return next(resources) if resources else {}

    def start_worker(self, project_key, sh_project,
                     process_id, items_to_process):
        '''
        @item_to_process: this would be a list of jobs in superclass
        but in our case we have handcoded 1 single item per worker.
        Gets the first item from items to process and ignore the rest.
        '''
        try:
            item_to_process = items_to_process[0]
            if not item_to_process:
                self.logger.info('No item to process, nothing to do.')
                return False

            item = json.dumps(
                item_to_process.get(self.args.consume_job_attribute)
            )
            previous_run = self.get_previous_run(item)
            job_args = {
                'project_key': project_key,
                'item': item,
                'retries': (
                    str(int(previous_run.get('retries', -1)) + 1)
                    if previous_run else '0'
                )
            }
            if not self.should_rerun_job(process_id, previous_run):
                return False

            if self.args.consumer_args:
                try:
                    self.logger.info(
                        'In consumer args: %s' % self.args.consumer_args
                    )
                    extra_args = loads(self.args.consumer_args)
                    job_args.update(extra_args)

                except (TypeError, ValueError):
                    self.logger.error(
                        'Could not add consumer args no valid JSON provided'
                    )
            resources = self.get_next_job_resource(process_id)
            if resources:
                job_args.update(resources)

            if not self.test_mode:
                self.logger.info('Started new job for %s' % job_args)
                sh_project.jobs.run(
                    spider=self.args.consumer, job_args=job_args,
                    add_tag=process_id,
                )
                self.add_job_to_tracker_collection(job_args.get('item'))

            self.logger.info('Started new job for item %s' % str(item))
            return True

        except Exception as err:
            self.logger.exception(err)

    def get_active_workers_spider_args(self):
        jobs = (
            self.sh_conn.sh_client.get_job(job_data)
            for job_data in get_jobs(
                self.sh_conn.sh_project, spider_name=self.args.consumer,
                status=['running', 'pending']
            )
        )
        return (
            job.metadata.get('spider_args')
            for job in jobs
        )

    def get_active_workers_items_in_use(self):
        return [
            spider_args.get('item')
            for spider_args in self.get_active_workers_spider_args()
        ]

    def parse_args(self):
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument(
            '--workers', type=int, default=DEFAULT_WORKERS,
            help='Number of parallel workers doing the processing'
        )
        parser.add_argument(
            '--consume-job-items', help='Job id to consume items from',
            required=True
        )
        parser.add_argument(
            '--consume-job-attribute',
            help='Item attribute name to consume from each job',
            required=True
        )
        parser.add_argument('--project')
        parser.add_argument('--test-mode')
        parser.add_argument(
            '--consumer', help='Spider name to consume and act as worker',
            required=True
        )
        parser.add_argument('--consumer-args', help='args to append to worker')
        parser.add_argument(
            '--begin', action='store_true',
            help='If specified we assume it will be the first run'
        )

        args = parser.parse_args()
        setattr(args, 'spider', args.consumer)
        setattr(args, 'spider_name', args.consumer)
        setattr(args, 'spider', args.consumer)
        setattr(args, 'jobs_per_worker', 1)
        return args


class InstacartParallelItemDispatcher(ParallelItemDispatcher):

    @property
    def logins(self):
        if not hasattr(self, '_logins'):
            users_fr = pkg_resources.resource_stream(
                'circleup', 'resources/users.json'
            )
            users = json.loads(users_fr.read()).get('resources_arguments')
            self._logins = cycle(users.values())
        return self._logins

    def resources(self, process_id):
        resources_in_use = self.get_resources_in_use(process_id)
        for login in self.logins:
            if login.get('user') not in resources_in_use:
                yield login

    def get_resources_in_use(self, process_id):
        return [
            spider_args.get('user')
            for spider_args in self.get_active_workers_spider_args()
        ]
