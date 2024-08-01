import json
import os
import platform
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from itertools import islice
from typing import Dict, List, Optional, Tuple, Any, Union

import aiohttp
import asyncio
from warnings import warn
from enum import IntEnum

VERSION = '0.8.1'


class TrancoList:
    def __init__(self, date: str, list_id: str, lst: List[str]) -> None:
        self.date: str = date
        self.list_id: str = list_id
        self.list_page: str = "https://tranco-list.eu/list/{}/".format(list_id)
        self.list: Dict[str, int] = {domain: index for index, domain in enumerate(lst, start=1)}

    def top(self, num: int = 1000000) -> List[str]:
        return sorted(self.list, key=self.list.get)[:num]

    def rank(self, domain: str) -> int:
        return self.list.get(domain, -1)


class TrancoCacheType(IntEnum):
    NOT_CACHED = 0
    CACHED_NOT_FULL = 1
    CACHED_FULL = 2


class Tranco:
    def __init__(self, **kwargs) -> None:
        """
        :param kwargs:
            cache_dir: <str> directory used to cache Tranco top lists, default: cwd + .tranco/
            account_email: <str> Account email address: retrieve from https://tranco-list.eu/account
            api_key: <str> API key: retrieve from https://tranco-list.eu/account
            http_proxy: <str> HTTP proxy URL (e.g., http://localhost:8080)
            socks5_proxy: <str> SOCKS5 proxy URL (e.g., socks5://localhost:1080)
        """

        self.cache_dir: Optional[str] = kwargs.get('cache_dir', None)
        if self.cache_dir is None:
            cwd = os.getcwd()
            self.cache_dir = os.path.join(cwd, '.tranco')
        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)
        self.cache_metadata: Dict[str, TrancoCacheType] = {}
        self._load_cache_metadata()

        self.account_email: str = kwargs.get('account_email')
        self.api_key: str = kwargs.get('api_key')

        # Proxy settings
        self.http_proxy: Optional[str] = kwargs.get('http_proxy')
        self.socks5_proxy: Optional[str] = kwargs.get('socks5_proxy')

        # Proxy URL configuration
        self.proxy = None
        if self.socks5_proxy:
            self.proxy = f'socks5://{self.socks5_proxy}'
        elif self.http_proxy:
            self.proxy = f'http://{self.http_proxy}'

        self.session: aiohttp.ClientSession = aiohttp.ClientSession(
            headers={'User-Agent': f'Python/{platform.python_version()} aiohttp/{aiohttp.__version__} tranco-python/{VERSION}'},
            proxy=self.proxy
        )

    def _cache_metadata_path(self) -> str:
        return os.path.join(self.cache_dir, 'metadata.json')

    def _cache_path(self, list_id) -> str:
        return os.path.join(self.cache_dir, f'{list_id}.csv')

    def _load_cache_metadata(self) -> None:
        if not os.path.exists(self._cache_metadata_path()):
            self._write_cache_metadata()
        with open(self._cache_metadata_path(), "rt") as f:
            self.cache_metadata = json.load(f)

    def _write_cache_metadata(self) -> None:
        with open(self._cache_metadata_path(), 'wt') as f:
            json.dump(self.cache_metadata, f)

    def _get_list_cache(self, list_id) -> TrancoCacheType:
        return self.cache_metadata.get(list_id, TrancoCacheType.NOT_CACHED)

    def _is_cached(self, list_id: Optional[str], full: bool = False) -> bool:
        if not list_id:
            raise ValueError("You must pass a list ID to cache a list.")
        list_cache: TrancoCacheType = self._get_list_cache(list_id)
        if list_cache == TrancoCacheType.NOT_CACHED:
            return False

        if full and (list_cache == TrancoCacheType.CACHED_NOT_FULL):  # need full, but full not present
            return False
        return True

    def _add_to_cache(self, list_id: Optional[str] = None, full: bool = False) -> None:
        if not list_id:
            raise ValueError("You must pass a list ID to cache a list.")
        self.cache_metadata[list_id] = max(TrancoCacheType.CACHED_FULL if full else TrancoCacheType.CACHED_NOT_FULL,
                                           self._get_list_cache(list_id))
        self._write_cache_metadata()

    def clear_cache(self) -> None:
        for f in os.listdir(self.cache_dir):
            os.remove(os.path.join(self.cache_dir, f))
        self._load_cache_metadata()

    async def list(self, date: Optional[str] = None, list_id: Optional[str] = None, subdomains: bool = False,
                   full: bool = False) -> TrancoList:
        """
        Retrieve a Tranco top list.
        :param date: Get the daily list for this date. If not given, the latest list is returned.
                     Combine with `subdomains` to select whether subdomains are included.
        :param list_id: Get the list with this ID. If neither the list ID nor date are given, the latest list is returned.
        :param subdomains: Include subdomains in the list. Only relevant when requesting a daily list. Default: False.
        :param full: Retrieve the full list (else only the top million). Default: False.
        :return: TrancoList object for the requested list.
        """
        if date and list_id:
            raise ValueError("You can't pass a date as well as a list ID.")
        if list_id and subdomains:
            warn("Subdomains parameter is ignored when passing a list ID.")

        if not list_id:
            if (not date) or (date == 'latest'):  # no arguments given: default to latest list
                yesterday = (datetime.utcnow() - timedelta(days=1))
                date = yesterday.strftime('%Y-%m-%d')
            list_id = await self._get_list_id_for_date(date, subdomains=subdomains)

        if not self._is_cached(list_id, full):
            await self._download_file(list_id, full)  # download list and load into cache
        with open(self._cache_path(list_id)) as f:  # read list from cache
            if full:
                top_list_lines = f.read().splitlines()
            else:
                top_list_lines = [line.rstrip() for line in islice(f, 1000000)]

        return TrancoList(date, list_id, list(map(lambda x: x[x.index(',') + 1:], top_list_lines)))

    async def _get_list_id_for_date(self, date: str, subdomains: bool = False) -> str:
        async with self.session.get(
            f'https://tranco-list.eu/daily_list_id?date={date}&subdomains={str(subdomains).lower()}'
        ) as response:
            if response.status == 200:
                return await response.text()
            else:
                raise AttributeError("The daily list for this date is currently unavailable.")

    async def _download_file(self, list_id: str, full: bool = False) -> None:
        if full:
            await self._download_full_file(list_id)
        else:
            await self._download_zip_file(list_id)
        self._add_to_cache(list_id, full)

    async def _download_zip_file(self, list_id: str) -> None:
        download_url = f'https://tranco-list.eu/download_daily/{list_id}'
        async with self.session.get(download_url) as response:
            if response.status == 200:
                with zipfile.ZipFile(BytesIO(await response.read())) as z:
                    with z.open('top-1m.csv') as csvf:
                        file_bytes = await csvf.read()
                        with open(self._cache_path(list_id), 'wb') as f:
                            f.write(file_bytes)
            elif response.status == 403:
                # List not available as ZIP file
                download_url = f'https://tranco-list.eu/download/{list_id}/1000000'
                async with self.session.get(download_url) as response2:
                    if response2.status == 200:
                        file_bytes = await response2.read()
                        with open(self._cache_path(list_id), 'wb') as f:
                            f.write(file_bytes)
                    else:
                        raise AttributeError("The daily list for this date is currently unavailable.")
            elif response.status == 502:
                # List unavailable (bad gateway)
                raise AttributeError("This list is currently unavailable.")
            else:
                # List unavailable (non-success status code)
                raise AttributeError("The daily list for this date is currently unavailable.")

    async def _download_full_file(self, list_id: str) -> None:
        download_url = f'https://tranco-list.eu/download/{list_id}/full'
        async with self.session.get(download_url) as response:
            if response.status == 200:
                file_bytes = await response.read()
                with open(self._cache_path(list_id), 'wb') as f:
                    f.write(file_bytes)

    async def configure(self, configuration: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Configure a custom list (https://tranco-list.eu/configure).
        Requires that valid credentials were passed when creating the `Tranco` object.
        :param configuration: dictionary that conforms to the schema at
        https://tranco-list.eu/api/configure
        :return: tuple (success: bool, message: str)
        """
        if not self.account_email or not self.api_key:
            raise ValueError("You need to provide `account_email` and `api_key` to configure a custom list.")
        
        async with self.session.post(
            'https://tranco-list.eu/configure',
            json=configuration,
            headers={
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
        ) as response:
            if response.status == 200:
                result = await response.json()
                return (True, result.get('message', 'Configuration successful'))
            else:
                return (False, await response.text())

    async def get_domain_ranks(self, domain: str) -> Dict[str, Any]:
        """
        Retrieve the ranks of a domain in the daily lists of the past 30 days.
        :param domain: The domain for which to query ranks.
        :return: Dictionary containing ranks information.
        :raises ValueError: If the domain is not valid or the request fails.
        """
        async with self.session.get(f'https://tranco-list.eu/ranks/domain/{domain}') as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 403:
                raise ValueError("Service temporarily unavailable.")
            elif response.status == 429:
                raise ValueError("Rate limit exceeded. Please try again later.")
            else:
                response.raise_for_status()
    
    async def close(self) -> None:
        """Close the aiohttp session."""
        await self.session.close()
