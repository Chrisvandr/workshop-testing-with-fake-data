import asyncio
import time
from abc import ABC
from collections.abc import Callable, Coroutine
from functools import cached_property
from http import HTTPStatus
from uuid import uuid4

import httpx
from httpx import AsyncClient, HTTPError
from structlog import get_logger
from structlog.contextvars import bound_contextvars

from shared.settings import settings

logger = get_logger(__name__)


class TooManyFailedRequestsError(Exception): ...


class RestClient:
    @cached_property
    def headers(self) -> dict[str, str]:
        return {}

    @property
    def hostname(self) -> str:
        return settings.hostname_apim

    def reset_headers_cache(self) -> None:
        if "headers" in self.__dict__:
            del self.__dict__["headers"]

    @staticmethod
    def check_status(response: httpx.Response) -> httpx.Response:
        try:
            response = response.raise_for_status()
        except HTTPError as err:
            logger.error(f"Request raised {response.status_code}: {err}")
            raise err
        return response


class SyncRestClient(RestClient, ABC):
    def get_request(
        self,
        request: str,
        include_hostname: bool,
        **kwargs,
    ) -> httpx.Response:
        url = f"{self.hostname}{request}" if include_hostname else f"{request}"

        return httpx.get(url, headers=self.headers, timeout=30.0, **kwargs)

    def send_request(
        self,
        request: str,
        retries: int = 0,
        check_status: bool = True,
        include_hostname: bool = True,
        **kwargs,
    ) -> httpx.Response:
        with bound_contextvars(
            request=request,
            request_id=str(uuid4()),
            retries_left=retries,
            request_kwargs=kwargs,
        ):
            logger.info("Sending request.")
            response = self.get_request(
                request,
                include_hostname=include_hostname,
                **kwargs,
            )
            logger.info("Response received.")

            if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR and retries > 0:
                return self.send_request(
                    request,
                    retries - 1,
                    check_status=check_status,
                    **kwargs,
                )
            if response.status_code == HTTPStatus.UNAUTHORIZED and retries > 0:
                self.reset_headers_cache()
                return self.send_request(
                    request,
                    retries - 1,
                    check_status=check_status,
                    **kwargs,
                )

            if check_status:
                return self.check_status(response)
            return response

    def get_paginated_results(  # noqa: PLR0913
        self,
        endpoint_func: Callable,
        break_condition: Callable,
        limit: int,
        retries: int,
        skip: int = 0,
        num_requests: int | None = None,
        sleep_time: int | None = None,
        **kwargs,
    ) -> list[dict]:
        """
        Get paginated results if endpoint supports it, requires
        the endpoint to have limit and skip parameters.

        Args:
            endpoint_func: A function that generates the endpoint URL with parameters.
            break_condition: A function that determines when to stop paginating.
            limit: The maximum number of items to retrieve per request.
            retries: The number of retries to attempt in case of failure.
            skip: The number of items to skip. Defaults to 0.
            num_requests: The maximum number of requests to make. Defaults to None.
            **kwargs: Additional keyword arguments to pass to the endpoint function.

        Returns:
            A list of results from the paginated requests.
        """
        results: list[dict] = []
        skip_count = skip
        while True:
            if num_requests is not None and num_requests < len(results) + 1:
                break

            request_url = endpoint_func(skip=skip_count, limit=limit, **kwargs)
            response = self.send_request(request_url, retries=retries)
            if response.status_code == HTTPStatus.NO_CONTENT:
                break  # NOTE if no content is returned assume we are done
            result = response.json()

            if break_condition(result):
                break

            results.append(result)
            skip_count += limit

            if sleep_time is not None:
                time.sleep(sleep_time)
        return results


class AsyncRestClient(RestClient, ABC):
    def __init__(self):
        self.client = AsyncClient(timeout=30.0)

    async def get_request(
        self,
        request: str,
        include_hostname: bool,
        **kwargs,
    ) -> httpx.Response:
        url = f"{self.hostname}{request}" if include_hostname else f"{request}"

        try:
            return await self.client.get(
                url,
                headers=self.headers,
                timeout=100,
                **kwargs,
            )
        except httpx.TimeoutException:
            logger.warning("request timed out")
            return httpx.Response(HTTPStatus.REQUEST_TIMEOUT)

    async def send_request(
        self,
        request: str,
        retries: int = 0,
        check_status: bool = True,
        include_hostname: bool = True,
        **kwargs,
    ) -> httpx.Response:
        with bound_contextvars(
            request=request,
            request_id=str(uuid4()),
            retries_left=retries,
            kwargs=kwargs,
        ):
            logger.info("Sending request.")
            response = await self.get_request(
                request,
                include_hostname=include_hostname,
                **kwargs,
            )
            logger.info("Response received.")

            if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR and retries > 0:
                return await self.send_request(
                    request,
                    retries - 1,
                    check_status,
                    **kwargs,
                )
            if response.status_code == HTTPStatus.UNAUTHORIZED and retries > 0:
                self.reset_headers_cache()
                return await self.send_request(
                    request,
                    retries - 1,
                    check_status,
                    **kwargs,
                )
            if check_status:
                return self.check_status(response)

            return response

    async def execute_tasks_in_batches(
        self,
        tasks: list[list[Coroutine]],
    ) -> list[httpx.Response]:
        """
        Execute a list of tasks in batches.

        Args:
            tasks: A list of lists, where each sublist contains
            coroutine objects to be executed.

        Returns:
            A list of responses from the executed coroutines.
        """
        if self.client.is_closed:
            self.client = AsyncClient()

        responses = []
        try:
            for batch in tasks:
                responses += await self.batch_request(batch)
        finally:
            await self.client.aclose()

        return responses

    @staticmethod
    async def batch_request(batch: list[Coroutine]) -> list[dict]:
        """
        Execute a batch of coroutines concurrently.

        Args:
            batch: A list of coroutine objects to be executed.

        Returns:
            A list of results from the executed coroutines.
        """
        return await asyncio.gather(*batch)
