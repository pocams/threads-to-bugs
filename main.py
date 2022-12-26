import asyncio
import json
import logging

from discord.utils import setup_logging

from discord_integration import DiscordClient
from google_integration import GoogleWorkbook


logger = logging.getLogger(__name__)


def discord_token(json_file="credentials/discord.json"):
    with open(json_file, "r") as f:
        return json.load(f)["token"]


async def main():
    setup_logging(level=logging.DEBUG)

    # https://docs.google.com/spreadsheets/d/1rOiUnof-UDy2cVIsM9cO6zhG7gfjThTcsdK2wCJsrPY/edit
    workbook = GoogleWorkbook("credentials/google-service-account.json", "1rOiUnof-UDy2cVIsM9cO6zhG7gfjThTcsdK2wCJsrPY")

    token = discord_token()
    client = DiscordClient("thread-home")
    discord_client_task = asyncio.create_task(client.start(token))

    # Initial sync
    threads = []
    async for thread in client.get_threads():
        threads.append(thread)

    logger.info(f"Doing initial sync of {len(threads)} threads")
    workbook.sync_threads(threads)

    while True:
        updated_thread = await client.thread_updates.get()
        logger.info(f"Syncing thread: {updated_thread}")
        workbook.sync_thread(updated_thread)

    await discord_client_task


if __name__ == "__main__":
    asyncio.run(main())
