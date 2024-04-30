from client.client import GoogleDriveClient
from consts import SHORTCUT_TYPE, logger, FOLDER_TYPE

SORT_DICT = {
    FOLDER_TYPE: 0,
    SHORTCUT_TYPE: 1,
}


def files_sort_key(file):
    fid, info = file
    return SORT_DICT.get(info["mimeType"], 10), info["name"], fid


class GoogleDriveBrowser(GoogleDriveClient):
    """Browse files in Google Drive

    Only owner's files are shown. Only supports browsing, no file operations are performed.
    """

    def __init__(self, secrets_dir, account_idx) -> None:
        super().__init__(secrets_dir, account_idx)

        self.copied_files = set()
        self.folders_copied = list()

    async def browse(self, root):
        stack = [root]
        namestack = [""]
        while stack:
            folder_id = stack[-1]

            # Get the list of files in the folder
            files = sorted(
                self.cache.get_folder_children(folder_id), key=files_sort_key
            )
            # Print the names of the files with numbers
            print(f'\n{"/".join(namestack)}')
            print("  0.  ..")
            for i, (file_id, file) in enumerate(files, start=1):
                line = f"  {i}.".ljust(6)
                line += file["name"]
                if file["mimeType"] == SHORTCUT_TYPE:
                    line += "@"

                line = line.ljust(50)
                size = int(file.get("size", self.cache.get_folder_size(file_id)))
                line += f"{size/ 2**20:.3f} MiB".ljust(20)
                line += file["createdTime"].ljust(30)
                line += file["modifiedTime"].ljust(30)
                line += file_id

                print(line)

            # Prompt the user to choose a file by number
            chosen_file_number = -1
            while not 0 <= chosen_file_number <= len(files):
                try:
                    chosen_file_number = int(
                        input("Enter the number of a file to open: ")
                    )
                except ValueError:
                    print("Invalid input. Please enter a number.")

            # If the user entered a number, try to open that file
            if chosen_file_number == 0:
                stack.pop()
                namestack.pop()
            else:
                file = files[chosen_file_number - 1]
                fileinfo = file[1]
                if fileinfo["mimeType"] == FOLDER_TYPE:
                    stack.append(file[0])
                    namestack.append(fileinfo["name"])
                elif fileinfo["mimeType"] == SHORTCUT_TYPE:
                    target_id = fileinfo["shortcutDetails"]["targetId"]
                    target_info = self.cache.file_info.get(target_id)
                    if target_info:
                        print(f"Shortcut to {target_info['name']}")
                        stack.append(target_id)
                        namestack.append(target_info["name"])
                    else:
                        logger.warning(f"Shortcut not in cache: {target_id}")
                else:
                    logger.warning(
                        f"{chosen_file_number}.  {fileinfo['name']} is not a folder"
                    )

    async def run(self, root: str = "root", orphans: bool = False):
        fields = {"createdTime", "modifiedTime"}

        if root == "root":
            logger.info("Browsing files in root folder")
            await self.cache.fetch("'me' in owners", shared=False, fields=fields)
            files = await self.cache.fetch_files(root, fields=fields)
            info = list(files.values())[0]
            # hack: add root as alias to root folder
            self.cache.file_info[root] = info
        else:
            await self.cache.fetch_folder_and_descendants(root, fields=fields)

        num_files = len(self.cache.file_info)
        logger.info(f"Number of files in GDrive: {num_files}")

        if orphans:
            await self.virtually_adopt_orphans(root)

        await self.browse(root)

    async def virtually_adopt_orphans(self, root):
        # await self.cache.fetch("'me' in owners", shared=False)
        # num_files = len(self.cache.file_info)
        # logger.info(f"Number of files in GDrive: {num_files}")
        logger.debug("Virtually adopting orphaned files...")

        for file_id, info in self.cache.get_orphan_files():
            logger.trace(
                f"Orphaned file: {file_id.ljust(80)}{info['name']}\t {info.get('parent', '')}"
            )
            info["parent"] = root
