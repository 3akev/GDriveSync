import json
import os
import sys

from google.oauth2.service_account import Credentials
from api.api_wrapper import GoogleDriveApiWrapper
from api.info_cache import InfoCache
from consts import logger, SCOPES


class GoogleDriveClient:
    def __init__(self, secrets_dir, account_idx) -> None:
        self.secrets_dir = secrets_dir

        self.accounts = {}

        if os.path.isdir(secrets_dir):
            secrets = sorted(
                os.path.join(secrets_dir, x) for x in os.listdir(secrets_dir) if x.lower().endswith(".json")
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
        email = [k for k, v in self.accounts.items() if v == secret][0]
        self._set_secret_by_email(email)

    def _set_secret_by_email(self, email):
        secret = self.accounts[email]

        self.email = email
        self.api = GoogleDriveApiWrapper(self._create_creds(secret))
        self.cache = InfoCache(self.api)
        logger.info(f"Using account {self.email}")

    def _create_creds(self, secret):
        credentials = Credentials.from_service_account_file(secret, scopes=SCOPES)
        return credentials

    async def run(self, *args, **kwargs):
        raise NotImplementedError("run method not implemented")
