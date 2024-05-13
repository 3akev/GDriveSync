import json
import os
import sys

from google.oauth2.service_account import Credentials
from api.api_wrapper import GoogleDriveApiWrapper
from api.info_cache import InfoCache
from consts import logger, SCOPES, CLIENT_SECRETS_FILE
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleDriveClient:
    def __init__(self, args) -> None:
        self.accounts = {}

        self.email: str
        self.api: GoogleDriveApiWrapper
        self.cache: InfoCache

        self.oauth = args.oauth

        creds = self.authenticate(args)
        self._set_secret_by_creds(creds)

    def authenticate(self, args):
        if args.oauth:
            credentials = self.authenticate_oauth()
        else:
            credentials = self.authenticate_service_account(args.secrets, args.account)
        return credentials

    def set_creds_for_secret(self, secret):
        with open(secret, "r") as f:
            email = json.load(f)["client_email"]
        creds = Credentials.from_service_account_file(secret, scopes=SCOPES)
        self.accounts[email] = creds
        return creds

    def authenticate_service_account(self, secrets_dir, account_idx):
        if os.path.isdir(secrets_dir):
            for file in (os.path.join(secrets_dir, x) for x in os.listdir(secrets_dir) if x.lower().endswith(".json")):
                self.set_creds_for_secret(file)

            creds = sorted(self.accounts.items(), key=lambda x: x[0])[account_idx - 1][1]

        elif os.path.isfile(secrets_dir):
            creds = self.set_creds_for_secret(secrets_dir)
        else:
            logger.error(f"Secrets file/dir not found: {secrets_dir}")
            sys.exit(1)

        return creds

    def authenticate_oauth(self):
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        credentials = flow.run_local_server()
        return credentials

    def _set_secret_by_creds(self, creds):
        email = None
        ls = [k for k, v in self.accounts.items() if v == creds]
        if ls:
            email = ls[0]
        self._set_secret(email, creds)

    def _set_secret_by_email(self, email):
        creds = self.accounts[email]
        self._set_secret(email, creds)

    def _set_secret(self, email, creds):
        self.email = email
        self.api = GoogleDriveApiWrapper(creds)
        self.cache = InfoCache(self.api)
        logger.info(f"Using account {self.email}")

    async def run(self, *args, **kwargs):
        raise NotImplementedError("run method not implemented")
