import asyncio
from typing import Any, Dict, List, Optional, Set

import google_auth_httplib2
import googleapiclient
import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient import discovery
from httplib2.error import HttpLib2Error

from api.request_batcher import GoogleDriveRequestBatcher
from consts import FOLDER_TYPE, SCOPES, SHORTCUT_TYPE, logger

DEFAULT_FIELDS = {
    "id",
    "size",
    "kind",
    "owners",
    "name",
    "mimeType",
    "parents",
    "shortcutDetails",
}


class GoogleDriveApiWrapper:
    def __init__(self, loop, secret) -> None:
        credentials = Credentials.from_service_account_file(secret, scopes=SCOPES)

        # Create a new Http() object for every request because httplib2 is not thread-safe
        # see: https://github.com/googleapis/google-api-python-client/blob/main/docs/thread_safety.md
        def build_request(http, *args, **kwargs):
            new_http = google_auth_httplib2.AuthorizedHttp(
                credentials, http=httplib2.Http()
            )
            return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)  # type: ignore

        authorized_http = google_auth_httplib2.AuthorizedHttp(
            credentials, http=httplib2.Http()
        )
        api = discovery.build(
            "drive",
            "v3",
            requestBuilder=build_request,  # type: ignore
            http=authorized_http,
        )
        self.batcher = GoogleDriveRequestBatcher(loop, api)
        self.files = api.files()
        self.about = api.about()

    ################################################################################
    # Raw API calls                                                                #
    ################################################################################
    async def get_about(self, *fields: str) -> dict:
        req = self.about.get(fields=",".join(fields or ["storageQuota"]))
        resp = await self.batcher.queue_request(req, execute_now=True)
        return resp

    async def clone_and_patch(self, file_id: str, **kwargs):
        req = self.files.copy(**{"fileId": file_id}, body=kwargs)
        cloned = await self.batcher.queue_request(req)
        return cloned

    async def get_file(self, file_id: str, fields: Optional[Set[str]] = None):
        req = self.files.get(
            fileId=file_id, fields=",".join(DEFAULT_FIELDS.union(fields or set()))
        )
        resp = await self.batcher.queue_request(req)
        return resp

    async def delete_file(self, file_id: str):
        req = self.files.delete(fileId=file_id)
        resp = await self.batcher.queue_request(req)
        return resp

    async def update(self, fileId: str, **kwargs):
        req = self.files.update(fileId=fileId, **kwargs)
        resp = await self.batcher.queue_request(req)
        return resp

    async def create(self, **kwargs):
        req = self.files.create(body=kwargs)
        resp = await self.batcher.queue_request(req)
        return resp

    async def fetch_file_info_one_page(
        self,
        page_token: Optional[str] = None,
        query: Optional[str] = None,
        shared: bool = True,
        fields: Optional[Set[str]] = None,
        batch: bool = False,
    ) -> Dict[str, Any]:
        kwargs = {
            "pageSize": 1000,
            "fields": f"files({','.join(DEFAULT_FIELDS.union(fields or set()))}), nextPageToken",
        }
        if page_token:
            kwargs["pageToken"] = page_token

        if query:
            kwargs["q"] = query

        req = self.files.list(
            supportsAllDrives=shared, includeItemsFromAllDrives=shared, **kwargs
        )

        # need to fetch page by page, requests aren't independent, so execute immediately
        resp = await self.batcher.queue_request(req, execute_now=not batch)

        return resp

    ################################################################################
    # Wrapper functions                                                            #
    ################################################################################
    async def fetch_all_file_info(
        self, query=None, shared=True, fields=None, batch=False
    ) -> List[Dict]:
        files = []

        init = True
        next_token = None
        while init or next_token:
            try:
                init = False
                if not batch:
                    logger.debug(
                        "Fetching page of file info with token = "
                        f"{next_token[:20] if next_token else None}..."
                    )
                resp = await self.fetch_file_info_one_page(
                    page_token=next_token,
                    query=query,
                    shared=shared,
                    fields=fields,
                    batch=batch,
                )
                next_token = resp.get("nextPageToken")
                files.extend(resp.get("files", []))
            except HttpLib2Error as e:
                logger.error(f"Error fetching file info: {e}. Retrying after pause...")
                await asyncio.sleep(5)

        return files

    async def create_folder(
        self, destination_parent_id: str, new_name: str, **kwargs
    ) -> dict:
        return await self.create(
            name=new_name,
            mimeType=FOLDER_TYPE,
            parents=[destination_parent_id],
            **kwargs,
        )

    async def copy_file(self, file_id: str, current: dict, destination_parent_id: str):
        moved = await self.clone_and_patch(
            file_id,
            parents=[destination_parent_id],
            name=current["name"],
            mime_type=current["mimeType"],
            createdTime=current["createdTime"],
            modifiedTime=current["modifiedTime"],
        )

        return moved

    async def create_shortcut(self, target_id, destination_id):
        shortcut_metadata = {
            "mimeType": SHORTCUT_TYPE,
            "shortcutDetails": {"targetId": target_id},
        }
        if destination_id:
            shortcut_metadata["parents"] = [destination_id]
        return await self.create(body=shortcut_metadata)

    async def get_files(self, *file_ids: str, fields=None):
        tasks = asyncio.as_completed(
            [self.get_file(file_id, fields=fields) for file_id in file_ids]
        )
        for res in tasks:
            yield await res
