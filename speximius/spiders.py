from os import environ
from json import loads, dumps
from six import string_types

from scrapy import Spider, Request, Item, Field
from scrapinghub import HubstorageClient

from circleup.utils.project import (
    get_project_settings, DEV_PROJECT_ID
)
from circleup.utils.cloud import sh_connection, post_to_collection
from circleup.settings import SCRAPINGHUB_APIKEY, PROJECT_ID
from circleup.utils.parallel.engines import (
    COLLS_API_URL, ParallelItemDispatcher
)

settings, project_conf = get_project_settings()


def check_if_string(txt):

    while True:
        if isinstance(txt, string_types):
            txt = txt.strip()
            if (txt.startswith('"') and txt.endswith('"')) \
                    or (txt.startswith("'") and txt.endswith("'")):
                txt = txt[1:-1]
            else:
                return txt
        else:
            return txt


class ParallelItemWorkerSpider(object):

    MINIMUM_EXPECTED_ITEMS = 100
    LOG_LINES = 100

    @property
    def job_id(self):
        if not hasattr(self, '_piw_job_id'):
            job_data = loads(environ.get('SHUB_JOB_DATA', '{}'))
            self._piw_job_id = job_data.get('key')
        return self._piw_job_id

    @property
    def seed(self):
        res = check_if_string(self.item)
        try:
            return loads(res.replace('\\"', '"'))
        except ValueError:
            self.logger.info('Seed Failed to JSON load')
            # string with no JSON structure
            return res

    @property
    def job(self):
        if not hasattr(self, '_piw_job'):
            if self.job_id:
                self._piw_job = self.sh_conn.sh_client.get_job(self.job_id)
            else:
                self._piw_job = None
        return self._piw_job

    @property
    def sh_conn(self):
        if not hasattr(self, '_piw_sh_conn'):
            self._piw_sh_conn = sh_connection(
                project_conf.get('project_id') or DEV_PROJECT_ID,
            )
        return self._piw_sh_conn

    @property
    def logs(self):
        if not hasattr(self, '_piw_logs'):
            if self.job:
                self._piw_logs = [
                    line for line in self.job.logs.iter(
                        count=self.LOG_LINES, offset=0
                    )
                ]
            else:
                self._piw_logs = []
        return self._piw_logs

    @property
    def requests(self):
        ''' We might have hundreds of thousand's of requests;
        return the requests iterator instead of a list of requests
        or a lazy request list that we store the first time we invoke.
        '''
        if self.job:
            return self.job.requests.iter()
        else:
            return []

    @property
    def job_retries(self):
        if not hasattr(self, 'retries'):
            self.retries = 0
        return self.retries

    def job_success_check(self, reason, item_count,
                          error_count, logs, requests):
        ''' Decide if the job was successful or not by analising the
        provided data. Most of the data could be derived from the spider
        but the idea is to make is as easier as possible for implementer
        to do job success checks.

        NOTE: Override per spider if more specific checks
        are required. The checks included are the basic, generic checks.

        args:
            - reason        :   string representing the job close reason
                                (eg: finished).
            - item_count    :   integer representing the total amount of items
                                scraped in the job.
            - error_count   :   integer representing the total amount of errors
                                found while executing the job.
            - logs          :   first 100 lines of job log as list of lines.
            - requests      :   all requests done by job as an iterator.

        @returns boolean    : True -> job is succesfull,
                              False -> job was a failure.
        '''
        if reason is not 'finished':
            return False

        if item_count == 0:
            return False

        if item_count <= self.MINIMUM_EXPECTED_ITEMS and error_count > 1:
            return False

        return True

    def closed(self, reason):
        ''' 1. Collect stats for the job that just finished
            2. Analise if the job is considered to be ok
            3. Update the job tracker collection accordingly
        '''
        try:
            # 1.
            stats = self.crawler.stats.get_stats()
            item_count = stats.get('item_scraped_count', 0)
            error_count = stats.get('log_count/ERROR', 0)

            # 2.
            job_success = self.job_success_check(
                reason, item_count, error_count, self.logs, self.requests
            )

            # 3.
            payload = dumps({
                '_key': ParallelItemDispatcher.generate_key(self.item),
                'success': job_success,
                'item_id': self.item, 'retries': self.job_retries
            })
            base_url = COLLS_API_URL % (
                dict(project_key=self.sh_conn.sh_project.key)
            )
            url = '%s/s/jobs_%s' % (base_url, self.crawler.spider.name)
            post_to_collection(url, payload)

        finally:
            self.sh_conn.sh_client.close()


class DiscoveryProcessorMixin(object):

    def get_previous_job(self, attr):
        if not hasattr(self, attr):
            raise AttributeError(
                'You should specify a {attr} argument to the job'.format(
                    attr=attr
                )
            )

        job_id = getattr(self, attr)
        auth = self.crawler.settings.get('SCRAPINGHUB_APIKEY')
        hc = HubstorageClient(auth=auth)
        return hc.get_job(job_id)

    def get_previous_job_items(self, attr):
        job = self.get_previous_job(attr)
        return job.items.iter_values()

    def get_previous_job_count(self, attr):
        job = self.get_previous_job(attr)
        return job.metadata.get('scrapystats', {}).get('item_scraped_count', 0)


class DiscoveryProcessorSpider(DiscoveryProcessorMixin, Spider):

    custom_settings = {
        'CRAWLERA_ENABLED': False,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': 400
        }
    }

    http_user = SCRAPINGHUB_APIKEY
    http_pass = ''

    project_checker_url = (
        'https://storage.scrapinghub.com/jobq/{project_id}/list'
    )

    def start_requests(self):
        '''
        If scrapinghub API is available we can go ahead with process
        '''
        self.project_id = (
            self.project_id if hasattr(self, 'project_id') else PROJECT_ID
        )
        yield Request(
            url=self.project_checker_url.format(project_id=PROJECT_ID),
            meta={'handle_httpstatus_list': [200, 403]},
            callback=self._process
        )

    def _process(self, response):
        '''
        Syntactic sugar to allow for a simple process method
        with no params in implementer
        '''
        for item in self.process():
            yield item

    def process(self):
        raise NotImplementedError(
            'You should implement a process method in this class'
        )


class DiscoveryItem(Item):
    data = Field()
