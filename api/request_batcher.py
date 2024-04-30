import asyncio
import functools
import random
import time
import traceback
import logging
from typing import Any

from consts import BACKOFF_RESET_SECONDS, BATCH_SIZE, logger


async def async_exec(loop, func, *args, **kwargs) -> Any:
    func = functools.partial(func, *args, **kwargs)
    result = await loop.run_in_executor(None, func)
    return result


class GoogleDriveRequestBatcher:
    def __init__(self, api) -> None:
        # NEEDS a running loop, only create this object after the loop is running
        self.loop = asyncio.get_running_loop()
        self.api = api

        self.batch_queue = []
        self.backoff_mult = 0
        self.backoff_start_time = 0
        self.backoff_now = False
        self.loop.create_task(self.do_queue_in_batches(self.batch_queue, persist=True))

    async def do_queue_in_batches(self, queue, persist=False):
        while persist or queue:
            # backoff once per (batched) request, even when whole batch breaks
            if self.backoff_now:
                self.backoff()
                self.backoff_now = False
                await self.wait_between_requests()
                continue

            if time.time() - self.backoff_start_time > BACKOFF_RESET_SECONDS:
                self.backoff_mult = 0

            try:
                if queue:
                    batch = queue[:BATCH_SIZE]
                    batreq = self.api.new_batch_http_request()

                    for b in batch:
                        batreq.add(b[0], callback=b[1])

                    await async_exec(self.loop, batreq.execute)

                    # remove batch *after* it has been successfully executed
                    # use del to avoid creating a new list (breaks reference to original list)
                    if id(queue) == id(self.batch_queue):
                        logger.debug(
                            f"Queue status: {len(batch)} requests, {len(queue) - len(batch)} remaining"
                        )
                    del queue[: len(batch)]

            except Exception as e:
                self.backoff()
                logger.warning(f"Error in request batching: {e}")
                if logger.getEffectiveLevel() < logging.DEBUG:
                    traceback.print_exc()

            finally:
                await self.wait_between_requests()

    async def wait_between_requests(self):
        # exponential backoff: https://developers.google.com/drive/api/guides/limits#exponential
        rand_milis = random.randint(1, 1000)
        wait_time = min(2**self.backoff_mult + rand_milis / 1000, 60)
        await asyncio.sleep(wait_time)

    def backoff(self):
        self.backoff_start_time = time.time()
        self.backoff_mult += 1
        logger.warning(f"Backing off for 2^{self.backoff_mult} seconds...")

    async def queue_request(self, req, execute_now=False) -> dict:
        # THIS IS BLACK MAGIC BUT IT WORKS SO WELL

        if not execute_now:
            # default queue, will be executed in persistent task
            queue = self.batch_queue
        else:
            # create ad-hoc queue for immediate execution
            queue = []

        future = self.loop.create_future()
        callback = self.make_callback(future, req)
        queue.append((req, callback))

        if queue is not self.batch_queue:
            await self.do_queue_in_batches(queue)

        # wait until future has a result, meaning when callback is called
        result = await future

        return result

    def make_callback(self, future, req):
        # this is called when the request is done
        def callback(request_id, response, exception):
            if exception is not None:
                self.handle_request_exception(future, exception, (req, callback))
            else:
                future.set_result(response)

        return callback

    def handle_request_exception(self, future, exception, queue_item):
        # 400 - Bad Request 	The request cannot be fulfilled due to a client error in the request.
        # 401 - Unauthorized 	The request contains invalid credentials.
        # 403 - Forbidden 	The request was received and understood, but the user doesn't have permission to perform the request.
        # 404 - Not Found 	The requested page couldn't be found.
        # 429 - Too Many Requests 	Too many requests to the API.
        # 500, 502, 503, 504 - Server Errors 	Unexpected error arises while processing the request.
        # from: https://developers.google.com/drive/api/guides/handle-errors
        if exception.status_code in [403, 429, 500, 502, 503, 504]:
            logger.warning(f"Error: {exception}. Retrying request...")
            # backoff once per batch
            self.backoff_now = True
            self.batch_queue.append(queue_item)

        elif exception.status_code in [400, 401, 404]:
            logger.warning(f"Unrecoverable error: {exception}. Skipping request...")
            future.set_result(None)
        else:
            logger.error(f"Unrecognized error: {exception}. Skipping request...")
            future.set_result(None)
