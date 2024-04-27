from datetime import datetime
from client.client import GoogleDriveClient
from consts import FOLDER_TYPE, logger

DATEFORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

PAIR_FIELDS = ["name", "createdTime", "mimeType"]


def parse_time(time_str):
    return datetime.strptime(time_str, DATEFORMAT)


class GoogleDriveDiff(GoogleDriveClient):
    async def run(self, first, second):
        fields = {"createdTime", "modifiedTime"}
        files = await self.cache.fetch_files(first, second, fields=fields)

        owners = {
            fid: owner.get("emailAddress")
            for fid, info in files.items()
            for owner in info.get("owners", [])
            if owner.get("emailAddress") in self.accounts
        }

        if len(owners) > 0:
            # if one of dirs is owned, use that account for better results
            owner = list(owners.values())[0]
            self._set_secret_by_email(owner)

        for _, owner in owners.items():
            await self.cache.fetch(f"'{owner}' in owners", shared=False, fields=fields)

        if len(owners) < 2:
            for fid in (first, second):
                if fid not in owners:
                    # SLOW AF but necessary so EVERYTHING is fetched
                    # otherwise google drive likes to skip some files on a whim
                    # if the request is too large
                    await self.cache.fetch_folder_and_descendants(fid, fields=fields)

        print("total files fetched:", len(self.cache.file_info))

        info1 = self.cache.file_info[first]
        info2 = self.cache.file_info[second]

        # diff stuff
        res = await self.compare((first, info1), (second, info2))

        self.print_diff(first, second, res)

    def print_diff(self, first, second, res):
        (new1, new2), (np1, np2) = res
        new1 = sorted((self.cache.build_path(fid, first), fid) for fid in new1)
        new2 = sorted((self.cache.build_path(fid, second), fid) for fid in new2)

        np1 = sorted((self.cache.build_path(fid, first), fid) for fid in np1)
        np2 = sorted((self.cache.build_path(fid, second), fid) for fid in np2)

        just = 120

        print("Newer in first:")
        for path, fid in new1:
            print("   ", path.ljust(just), f"({fid})")
        print()
        print("Newer in second:")
        for path, fid in new2:
            print("   ", path.ljust(just), f"({fid})")
        print()
        print("Only in first:")
        for path, fid in np1:
            print("   ", path.ljust(just), f"({fid})")
        print()
        print("Only in second:")
        for path, fid in np2:
            print("   ", path.ljust(just), f"({fid})")

    async def compare(self, first, second):
        first, info1 = first
        second, info2 = second
        if (
            info1.get("mimeType") == FOLDER_TYPE
            and info2.get("mimeType") == FOLDER_TYPE
        ):
            children1 = list(self.cache.get_folder_children(first, filter_ignored=True))
            children2 = list(
                self.cache.get_folder_children(second, filter_ignored=True)
            )

            paired1 = set()
            paired2 = set()
            pairs = []

            for fid1, info1 in children1:
                for fid2, info2 in children2:
                    if self.are_paired(info1, info2):
                        pairs.append((fid1, info1, fid2, info2))
                        paired1.add(fid1)
                        paired2.add(fid2)

            not_paired1 = set(x[0] for x in children1) - paired1
            not_paired2 = set(x[0] for x in children2) - paired2

            acc1, acc2 = [], []
            for fid1, info1, fid2, info2 in pairs:
                (new1, new2), (np1, np2) = await self.compare(
                    (fid1, info1), (fid2, info2)
                )
                acc1.extend(new1)
                acc2.extend(new2)

                not_paired1 = not_paired1.union(np1)
                not_paired2 = not_paired2.union(np2)

            return (acc1, acc2), (not_paired1, not_paired2)

        else:
            compare = self.compare_files(first, info1, second, info2)

            if compare > 0:
                return ([first], []), ([], [])
            elif compare < 0:
                return ([], [second]), ([], [])
            else:
                return ([], []), ([], [])

    def are_paired(self, info1, info2):
        return all(info1.get(k) == info2.get(k) for k in PAIR_FIELDS)

    def compare_files(self, first, info1, second, info2) -> int:
        """
        Returns >0 if first is newer, <0 if second is newer, 0 if same,
        """
        # parse dates
        date1 = parse_time(info1["modifiedTime"])
        date2 = parse_time(info2["modifiedTime"])
        if date1 == date2:
            # logger.trace(f"{info1['name']} ({first}) and ({second}) equal")
            return 0
        elif date1 > date2:
            logger.trace(f"{info1['name']} ({first}) is newer than ({second})")
            return 1
        else:
            logger.trace(f"{info2['name']} ({second}) is newer than ({first})")
            return -1
