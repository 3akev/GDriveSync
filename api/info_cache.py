import asyncio
from collections import defaultdict
from functools import reduce
from typing import Dict, Optional

from api.api_wrapper import GoogleDriveApiWrapper
from consts import FOLDER_TYPE, IGNORE_LIST, logger, SHORTCUT_TYPE


class InfoCache:
    def __init__(self, api: GoogleDriveApiWrapper):
        self.api = api
        self.file_info: Dict = defaultdict(dict)

        self._folder_sizes_cache = {}
        self._paths_cache = {}

    def is_ignored(self, name: str) -> Optional[str]:
        if name in IGNORE_LIST:
            return name
        return None

    def clear(self):
        self.file_info.clear()
        self._folder_sizes_cache.clear()
        self._paths_cache.clear()

    def _memoize(self, func, cache, *args):
        key = tuple(args)
        if key not in cache:
            cache[key] = func(*args)
        return cache[key]

    ################################################################################
    # Queries                                                                      #
    ################################################################################
    def get_folder_children(self, folder_id: str, filter_ignored=False):
        info = self.file_info[folder_id]
        if info.get("mimeType") == SHORTCUT_TYPE:
            folder_id = info["shortcutDetails"]["targetId"]

        for file_id, file in self.file_info.items():
            if filter_ignored and self.is_ignored(file.get("name")):
                continue
            if file.get("parent") == folder_id:
                yield file_id, file

    def get_files_in_hierarchy(self, folder_id: str, filter_ignored: bool = True):
        for file_id, file in self.file_info.items():
            if filter_ignored and self.is_ignored(file.get("name")):
                continue
            if file.get("parent") == folder_id:
                yield file_id, file
                if file.get("mimeType") == FOLDER_TYPE:
                    yield from self.get_files_in_hierarchy(file_id)
                elif file.get("mimeType") == SHORTCUT_TYPE:
                    yield from self.get_files_in_hierarchy(
                        file["shortcutDetails"]["targetId"]
                    )

    def is_owned_by_me(self, file_id):
        return any(x["me"] for x in self.file_info.get(file_id, {}).get("owners", []))

    def get_orphan_files(self):
        for file_id, file in self.file_info.items():
            if self.is_owned_by_me(file_id) and (
                file.get("parent") is None
                or not self.is_owned_by_me(file.get("parent"))
            ):
                yield file_id, file

    def get_folder_size(self, folder_id: str):
        return self._memoize(self._get_folder_size, self._folder_sizes_cache, folder_id)

    def _get_folder_size(self, folder_id: str):
        return reduce(
            lambda x, y: x + int(self.get_file_size(y[0])),
            self.get_folder_children(folder_id),
            0,
        )

    def get_num_folders(self, folder_id: str, filter_ignored=True):
        return sum(
            1
            for x in self.get_files_in_hierarchy(
                folder_id, filter_ignored=filter_ignored
            )
            if x[1].get("mimeType") == FOLDER_TYPE
        )

    def build_path(self, file_id: str, stop_at: Optional[str] = None):
        return self._memoize(self._build_path, self._paths_cache, file_id, stop_at)

    def _build_path(self, file_id: str, stop_at: Optional[str] = None):
        info = self.file_info[file_id]
        path = info["name"]
        parent_id = info.get("parent")
        while parent_id in self.file_info and parent_id != stop_at:
            info = self.file_info[parent_id]
            path = info["name"] + "/" + path
            parent_id = info.get("parent")
        return "/" + path

    def get_file_size(self, file_id: str):
        file = self.file_info[file_id]
        if file.get("mimeType") in [FOLDER_TYPE, SHORTCUT_TYPE]:
            return self.get_folder_size(file_id)
        else:
            return self.file_info[file_id].get("size", 0)

    ################################################################################
    # Fetching                                                                     #
    ################################################################################
    async def fetch_files(self, *file_ids: str, fields=None):
        async for file in self.api.get_files(*file_ids, fields=fields):
            parsed = self.parse_files(file)
            k, v = parsed.popitem()
            self.file_info[k].update(v)
            # self.file_info[file_ids].update(v)
        logger.debug(f"Fetched file info for id='{file_ids}'")
        return {x: self.file_info[x] for x in file_ids}

    async def fetch(self, query=None, shared=True, fields=None, batch=False):
        if not batch:
            logger.debug(
                f"Fetching file info from GDrive with query = [{query}] and shared = {shared} and fields = {fields}"
            )
        files = await self.api.fetch_all_file_info(
            query, shared, fields=fields, batch=batch
        )
        new = self.parse_files(*files)
        self.file_info.update(new)
        if not batch:
            logger.debug(f"Fetched and parsed {len(new)} files.")
        return new

    async def fetch_folder_and_descendants(self, folder_id: str, fields=None):
        logger.debug(f"Fetching folder and descendants for id='{folder_id}'")
        new = await self.fetch_files(folder_id, fields=fields)
        await self.fetch_descendants(folder_id, fields=fields)

    async def fetch_descendants(self, folder_id: str, fields=None):
        new = await self.fetch(f"'{folder_id}' in parents", fields=fields, batch=True)
        tasks = []
        for file_id, file in new.items():
            if file.get("mimeType") == FOLDER_TYPE and not self.is_ignored(
                file.get("name")
            ):
                tasks.append(self.fetch_descendants(file_id, fields=fields))
        await asyncio.gather(*tasks)

    def parse_files(self, *files: Dict) -> Dict[str, Dict]:
        file_id_to_info = defaultdict(dict)
        for file in files:
            file_id = file.pop("id")
            for k, v in file.items():
                if k == "parents":
                    file_id_to_info[file_id]["parent"] = v[0]
                else:
                    file_id_to_info[file_id][k] = v
        return file_id_to_info
