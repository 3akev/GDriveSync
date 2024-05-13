import asyncio
from typing import Optional

from client.cleaner import GoogleDriveCleaner
from client.cloner import GoogleDriveCloner
from consts import logger


class GoogleDriveRotator(GoogleDriveCloner, GoogleDriveCleaner):
    async def run(  # type: ignore
        self,
        base_folder_id: str,
        destination_parent_folder_id: str,
        new_name: Optional[str] = None,
        dry_run: bool = False,
    ):
        fields = {"createdTime"}
        await self.cache.fetch(f"'{destination_parent_folder_id}' in parents", fields=fields)
        backups = list(self.cache.file_info.items())

        if len(backups) >= len(self.accounts):
            logger.info("Cleaning up oldest backup")
            sorted_backups = sorted(backups, key=lambda x: x[1]["createdTime"])

            oldest = sorted_backups[0]
            picked = oldest[1]["owners"][0]["emailAddress"]

            logger.info(f"Deleting oldest backup {oldest[1]['name']} ({oldest[0]}) owner: {picked}")

            self._set_secret_by_email(picked)

            await self.cache.fetch_files(oldest[0])
            await self.clean(oldest[0], dry_run=dry_run)

            # remove cache from cleaning, just in case it breaks things
            self.cache.clear()

            logger.info("Cleanup done. Waiting 10 seconds for drive to catch up...")
            await asyncio.sleep(10)
        else:
            logger.info("Less backups than accounts, picking unused account")
            emails = [y[1]["owners"][0]["emailAddress"] for y in backups]
            unused = [x for x in self.accounts if x not in emails]

            picked = unused[0]
            self._set_secret_by_email(picked)

        await self.clone(
            base_folder_id=base_folder_id,
            destination_parent_folder_id=destination_parent_folder_id,
            new_name=new_name,
            dry_run=dry_run,
        )
