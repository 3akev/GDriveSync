from client.client import GoogleDriveClient


def format_storage_quota(storage_quota) -> str:
    return "\n".join(
        f"{str(k.ljust(30))}: {int(v) / 2 ** 30:.3f} GiB"
        for k, v in storage_quota["storageQuota"].items()
    )


class GoogleDriveQuota(GoogleDriveClient):
    async def run(self):
        print(format_storage_quota(await self.api.get_about()))
