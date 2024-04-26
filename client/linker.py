from client.client import GoogleDriveClient


class GoogleDriveLinker(GoogleDriveClient):
    async def run(self, target_id, destination_id):
        await self.api.create_shortcut(target_id, destination_id)
