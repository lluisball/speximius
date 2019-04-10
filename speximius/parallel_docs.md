## How to distribute job results to parallel jobs ##

To distribute job results to parallel workers we have two distinct pieces:

### Dispatcher: ParallelItemDispatcher class ###

Is available through the script located in bin/parallel\_item\_dispatcher.py

Allowed parameters for the script are:

* workers: amount of workers that we want to have running in parallel.
* consume-job-items: take the items generated in the job key specified.
* consume-job-attribute: take the attribute specified from each item as the seed for each consumer job.
* consumer: spider name that is going to receive a seed and be parallelised.
* consumer-args: a json serialized dictionary with args to provide to consumer spider.

        $> parallel_item_dispatcher.py                      \
        --workers=25                                        \
        --consume-job-items=168012/131/2                    \
        --consume-job-attribute=store_source_id             \
        --consumer-args={"products_job": "168012/129/5"}    \
        --project=168012                                    \
        --consumer=bedbathandbeyond_availability

This script execution is specifying the following:

Extract all the items from job `168012/131/2`, for each item found create a job for spider `bedbathandbeyond_availability` and pass to the job the value stored behind the attribute `store\_source\_id` of the item. Have a maximum of 25 jobs running at any given time. Take the dictionary `{"products_job": "168012/129/5"}` and pass the k/v pairs to each job that will be created as spider args.

Additionally we can specify that the execution is a new exectution or a re-rerun / continuation from previous run. To do this we have the argument `--begin`.

* If specified (`--begin`), the process will be fully executed from scratch and no tracker data from previous executions will be used.
* If not specified, the process will just execute what ever left to be exectuted or what failed in the previous runs.

A job will be retried a maximum of 2 times if is marked as a bad job. How a job is established to be a failure depends on the check set by default or overriden by each spider when required some non standard checks.


### Spider: ParallelItemWorkerSpider ###

Any spider that we would like to execute in parallel by consuming the items from a job will have inherint the class `circleup.utils.parallel.spiders.ParallelItemWorkerSpider`

Each job generated will have the item's selected attribute as the spider argument `item` and therefore the item string will be available inside the spider's `self.item`.

When a job is finished (regardless of the reason) the method `job_success_check` in invoked. By default, very simple checks are done to establish if the job was successful. If the job has no items or in case of having many errors and a small quantity of items or if the job finished with a status different to `finished` (eg: `failed`) will be marked as a failure.

This job success check can be overridden by implementing the `job_success_check` method. End reason, item and error counts, logs and requests made will be available in the method's input parameters. Using this we can implement much more specific checks that will for instance look for specific strings in the job log.

As an example, in order to implement this within a spider:

1. Inherit parallelisation class:

        from circleup.utils.parallel.spiders import ParallelItemWorkerSpider

        class BedbathandbeyondAvailability(ParallelItemWorkerSpider,
                                           AvailabilitySpider,
                                           BedbathandbeyondProducts):

2. Expect the instance variable `item` set in the spider:

        def start_requests(self):

            # Extract the provided item id which matches a store id in the
            # bedbathandbeyond_stores job referenced in engine.
            store_id = self.item

            for brand in self.product_iter:
                if not brand.get('brand_source_id'):
                    continue

                self.logger.info(
                    'Following for store_id %s and brand_id %s' % (
                        store_id, brand.get('brand_source_id')
                    )
                )
                yield Request(
                    url=brand.get('source_url'),
                    callback=self.parse_brand,
                    meta={'store_id': store_id},
                    headers=self.DEFAULT_HEADERS
                )

    * In this case, we are adding the store\_id in the meta and later on during the process all data will be extracted explicitly just for this store.
    * Each parallelised job will receive a different store\_id in this case.

3. As optional step you can establish how is the spider going to self know if it was a success. To do this just override in your spider the following method inherited via `ParallelItemWorkerSpider`:

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
                - requests      :   all requests done by job as an iterator


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
