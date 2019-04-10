'''
Helper functions to access the scrapy cloud API in a
more convenient way than raw access
'''

import json
import os

from time import sleep
from requests import post, get
from requests.auth import HTTPBasicAuth

from scrapinghub import ScrapinghubClient


JOBS_PER_PAGE = 1000
ITEMS_PER_PAGE = 4000
MAX_COLLECTION_RETRIES = 60


class SHConnection():
    ''' Wrapper for scrapinghub client, project and api calls
    to simplify use.
    '''

    def __init__(self, api_key, default_project_key=None):
        self.api_key = api_key
        self.project_key = resolve_project_key(
            default_project_key=default_project_key
        )

    def __enter__(self):
        self.client = ScrapinghubClient(self.api_key)
        self.project = self.client.get_project(self.project_key)
        return self

    def __exit__(self, *args):
        self.client.close()

    def jobs_iter(self, **kwargs):
        return self.project.jobs.iter(**kwargs)

    def get_job(self, job_id):
        return self.client.get_job(job_id)


def resolve_project_key(default_project_key=None):
    try:
        data = json.loads(os.environ['SHUB_JOB_DATA'])
        project_key = data['project']
        return project_key

    except Exception:
        if default_project_key:
            return default_project_key
        raise NameError(
            'When executing in dev local environments you must'
            ' provide a project key'
        )


def get_jobs(sh_connection, spider_name=None, has_tag=None,
             lacks_tag=None, status=None, all_job=False, startts=None,
             count=None):
    start = 0
    job_count = 0
    max_count = None if not count else int(count)
    while True:
        kwargs = dict(
            lacks_tag=lacks_tag, has_tag=has_tag,
            start=start, count=max_count or 1000, startts=startts
        )
        if spider_name:
            kwargs['spider'] = spider_name
        if status:
            kwargs['state'] = status

        has_jobs = False
        for job in jobs_iter(sh_connection, **kwargs):
            job_count += 1
            job_key = job.get('key')
            yield (job_key if not all_job else job)
            has_jobs = True

        if not has_jobs or job_count < (max_count or JOBS_PER_PAGE):
            break

        job_count = 0
        start += JOBS_PER_PAGE


def jobs_iter(sh_connection, **kwargs):
    served_jobs = []
    while True:
        try:
            jobs = sh_connection.jobs_iter(**kwargs)
            for job in jobs:
                if job not in served_jobs:
                    served_jobs.append(job)
                    yield job
            return
        except Exception:
            # scrapy cloud bad status line or some similar situation
            sleep(1)


def get_undelivered_jobs(sh_connection, spider_name, lacks_tags=None):
    tags = ['delivered']
    if lacks_tags:
        tags.extend(lacks_tags)
    return get_jobs(
        sh_connection, spider_name, lacks_tag=tags
    )


def get_job_items(sh_connection, job_id, logger=None):
    if logger:
        logger.info('Getting job: %s' % job_id)

    start_indx = 0
    has_items = False
    job = sh_connection.get_job(job_id)

    while True:
        has_items = False
        start = '%s/%s' % (job_id, start_indx)
        for item in job_items_iter(job, start, ITEMS_PER_PAGE):
            has_items = True
            yield item
        if not has_items:
            break
        start_indx += ITEMS_PER_PAGE


def job_items_iter(job, start, count):
    ''' Retry forever not yielding alredy yieled items
    This is to avoid 'Too many requests' errors or
    'bad status line' errors
    '''
    items_served = []
    while True:
        try:
            for item in job.items.iter(start=start, count=ITEMS_PER_PAGE):
                if item not in items_served:
                    items_served.append(item)
                    yield item
            return
        except Exception:
            sleep(1)


def post_to_collection(sh_connection, collection_url, payload,
                       logger=None, apikey=None):
    apikey = sh_connection.api_key
    tries = 0
    while True and tries < MAX_COLLECTION_RETRIES:
        tries += 1
        response = post(
            collection_url, data=payload, auth=HTTPBasicAuth(apikey, '')
        )
        if response.ok or response.status_code in [404]:
            if logger:
                logger.info(
                    'Added to collection %s to payload %s' % (
                        collection_url, str(payload)
                    )
                )
            return
        if logger:
            logger.warning('Collection POST failed, retry %d' % tries)
            logger.warning(str(response))
        sleep(1)


def get_collection_item(sh_connection, collection_url, collection_id,
                        logger=None, apikey=None):
    apikey = sh_connection.api_key
    tries = 0
    while True:
        tries += 1
        url = '%s/%s' % (collection_url, collection_id)
        response = get(url, auth=HTTPBasicAuth(apikey, ''))
        if response.ok:
            if logger:
                logger.info('Got collection item %s' % (collection_id))
            try:
                return json.loads(response.text)
            except ValueError:
                return None
        elif response.status_code == 404:
            return None

        if logger:
            logger.warning('Collection GET failed, retry %d' % tries)
            logger.warning(str(response))
        sleep(1)
