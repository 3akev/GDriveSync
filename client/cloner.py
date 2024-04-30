import asyncio
import time
from typing import Optional

from client.client import GoogleDriveClient
from consts import FOLDER_TYPE, logger
from tqdm.asyncio import tqdm


class GoogleDriveCloner(GoogleDriveClient):
    def __init__(self, secrets_dir, account_idx) -> None:
        super().__init__(secrets_dir, account_idx)
        self.num_folders_to_copy = 0
        self.files_to_copy = []

        self.files_copied = set()
        self.folders_copied = set()

    @property
    def copied_total(self):
        return len(self.files_copied) + len(self.folders_copied)

    async def copy_folder_structure(
        self,
        folder_id: str,
        destination_parent_id: str,
        new_name: Optional[str] = None,
        pbar=None,
        dry_run: bool = False,
    ) -> Optional[str]:
        item_info = self.cache.file_info[folder_id]

        if self.cache.is_ignored(item_info["name"]):
            logger.trace(f"skipping {item_info['name']}")
            return None

        if folder_id in self.folders_copied:
            return None

        if item_info["mimeType"] == FOLDER_TYPE:
            created_folder_id = ""
            if not dry_run:
                fields = {}
                # if not root folder, copy dates; root folder dates have to be new so rotation works
                if not new_name:
                    fields["createdTime"] = item_info["createdTime"]
                    fields["modifiedTime"] = item_info["modifiedTime"]
                result = await self.api.create_folder(
                    destination_parent_id=destination_parent_id,
                    new_name=new_name or item_info["name"],
                    **fields,
                )
                created_folder_id = result["id"]
            self.folders_copied.add(folder_id)

            if pbar:
                pbar.update(1)
            await self.log_item_copied(
                folder_id, len(self.folders_copied), self.num_folders_to_copy
            )

            results = []
            for file_id, file_info in self.cache.get_folder_children(folder_id):
                if file_info["mimeType"] == FOLDER_TYPE:
                    results.append(
                        self.copy_folder_structure(
                            file_id, created_folder_id, pbar=pbar, dry_run=dry_run
                        )
                    )
                else:
                    # results.append(self.copy_file(file_id, created_folder_id))
                    self.files_to_copy.append((file_id, created_folder_id))

            await asyncio.gather(*results)

            return created_folder_id

    async def copy_file(
        self, file_id: str, destination_parent_id: str, dry_run: bool = False
    ):
        item_info = self.cache.file_info[file_id]

        if self.cache.is_ignored(item_info["name"]):
            logger.trace(f"skipping {item_info['name']}")
            return None

        if file_id in self.files_copied:
            return None

        if item_info["mimeType"] != FOLDER_TYPE:
            new_file_id = ""
            if not dry_run:
                new_file_id = await self.api.copy_file(
                    file_id=file_id,
                    current=self.cache.file_info[file_id],
                    destination_parent_id=destination_parent_id,
                )
            self.files_copied.add(file_id)
            await self.log_item_copied(
                file_id, len(self.files_copied), len(self.files_to_copy)
            )
            return new_file_id

    async def log_item_copied(self, item_id, current, total):
        logger.trace(
            f"Copied {self.cache.build_path(item_id).ljust(120)}"
            f" {item_id.ljust(40)} {current}/{total}"
        )

    async def clone(
        self,
        base_folder_id: str,
        destination_parent_folder_id: str,
        new_name: Optional[str] = None,
        dry_run: bool = False,
    ):
        # fetch time to copy over, so diff works
        fields = {"createdTime", "modifiedTime"}
        await self.cache.fetch_folder_and_descendants(base_folder_id, fields=fields)
        num_files = len(self.cache.file_info)
        logger.info(f"Number of files fetched: {num_files}")

        items_to_copy = list(self.cache.get_files_in_hierarchy(base_folder_id))
        logger.info(f"Number of items to copy: {len(items_to_copy)}")

        self.num_folders_to_copy = self.cache.get_num_folders(base_folder_id)
        logger.info(f"Number of folders to copy: {self.num_folders_to_copy}")

        size_to_copy = self.cache.get_folder_size(base_folder_id)
        logger.info(f"Size to copy: {size_to_copy / 2 ** 30:.3f} GiB")

        about = await self.api.get_about()
        quota = about["storageQuota"]
        free = int(quota["limit"]) - int(quota["usage"])
        if size_to_copy > free:
            logger.error(
                f"Insufficient space. Free: {free / 2 ** 30:.3f} GiB,"
                f" Needed: {size_to_copy / 2 ** 30:.3f} GiB. "
                "Exiting..."
            )
            return

        logger.info("Copying folder structure...")
        with tqdm(
            total=self.num_folders_to_copy,
            miniters=1,
            maxinterval=1,
            unit="folders",
            colour="green",
        ) as pbar:
            await self.copy_folder_structure(
                base_folder_id, destination_parent_folder_id, new_name, pbar, dry_run
            )

        tasks = [self.copy_file(x, y, dry_run=dry_run) for x, y in self.files_to_copy]
        logger.info(f"Number of files to copy: {len(tasks)}")
        logger.info("Copying files...")
        await tqdm.gather(
            *tasks,
            miniters=1,
            maxinterval=1,
            unit="files",
            colour="green",
        )
        logger.info("Done")

    async def run(
        self,
        base_folder_id: str,
        destination_parent_folder_id: str,
        new_name: Optional[str] = None,
        dry_run: bool = False,
    ):
        await self.clone(
            base_folder_id, destination_parent_folder_id, new_name, dry_run
        )
