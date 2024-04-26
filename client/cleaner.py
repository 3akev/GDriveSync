from client.client import GoogleDriveClient
from consts import logger

from tqdm.asyncio import tqdm


class GoogleDriveCleaner(GoogleDriveClient):
    async def delete_own_files(self, file_ids, dry_run):
        tasks = []
        for file_id in file_ids:
            info = self.cache.file_info[file_id]
            if self.cache.is_owned_by_me(file_id):
                logger.trace(f"will delete {info['name']} ({file_id})")
                if not dry_run:
                    tasks.append(self.api.delete_file(file_id))
            else:
                logger.trace(f"won't delete {info['name']} ({file_id}) not owned by me")

        logger.info(f"deleting {len(tasks)} files")

        await tqdm.gather(*tasks, miniters=1)

    async def clean(self, *file_ids, dry_run=False):
        if file_ids[0] == "all":
            await self.delete_own_files(self.cache.file_info, dry_run=dry_run)
        else:
            await self.delete_own_files(file_ids, dry_run=dry_run)

    async def run(self, *file_ids, dry_run=False):
        if file_ids[0] == "all":
            await self.cache.fetch("'me' in owners", shared=False)
            if input("Are you sure you want to delete all files? (yes/no): ") != "yes":
                return
        else:
            await self.cache.fetch_files(*file_ids)
            print("Files to delete:")
            print(
                "\n".join(f"{x}: {self.cache.file_info[x]['name']}" for x in file_ids)
            )

            if (
                input("Are you sure you want to delete these files? (yes/no): ")
                != "yes"
            ):
                return

        await self.clean(*file_ids, dry_run=dry_run)
