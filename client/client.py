import asyncio
import json
import os
import sys

from api.api_wrapper import GoogleDriveApiWrapper
from api.info_cache import InfoCache
from consts import logger


class GoogleDriveClient:
    def __init__(self, loop, secrets_dir, account_idx) -> None:
        self.secrets_dir = secrets_dir

        self.accounts = {}
        if not loop:
            loop = asyncio.get_event_loop()
        self.loop = loop

        if os.path.isdir(secrets_dir):
            secrets = sorted(
                os.path.join(secrets_dir, x)
                for x in os.listdir(secrets_dir)
                if x.lower().endswith(".json")
            )
            for file in secrets:
                with open(file, "r") as f:
                    self.accounts[json.load(f)["client_email"]] = file

            secret = secrets[account_idx - 1]

        elif os.path.isfile(secrets_dir):
            with open(secrets_dir, "r") as f:
                self.accounts[json.load(f)["client_email"]] = secrets_dir
            secret = secrets_dir
        else:
            logger.error(f"Secrets file/dir not found: {secrets_dir}")
            sys.exit(1)

        self.email: str
        self.api: GoogleDriveApiWrapper
        self.cache: InfoCache

        self._set_secret(secret)

    def _set_secret(self, secret):
        self.email = [k for k, v in self.accounts.items() if v == secret][0]
        self.api = GoogleDriveApiWrapper(self.loop, secret)
        self.cache = InfoCache(self.api)
        logger.info(f"Using account {self.email}")

    async def fetch_shared_files(self, accounts, fields=None):
        query = " or ".join([f"'{x}' in owners" for x in accounts])
        query = f"not ({query})"
        return await self.cache.fetch(query, shared=True, fields=fields)

    async def run(self, *args, **kwargs):
        raise NotImplementedError("run method not implemented")
