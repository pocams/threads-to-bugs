import asyncio
import logging

import discord

from data import Thread

logger = logging.getLogger(__name__)


class DiscordClient(discord.Client):
    intents = discord.Intents.default()
    intents.message_content = True
    intents.guilds = True

    def __init__(self, channel_name):
        super().__init__(intents=self.intents)
        self.channel_name = channel_name
        self.channel: discord.ForumChannel = None
        self.is_ready = asyncio.Event()
        self.thread_updates = asyncio.Queue()

    async def message_or_none(self, thread, message_id):
        try:
            return await thread.fetch_message(message_id)
        except discord.errors.NotFound:
            return None

    async def on_ready(self):
        logger.info(f"Logged in as {self.user!r}")
        for channel in self.get_all_channels():
            if channel.name == self.channel_name:
                print(f"Found our channel: {channel}")
                self.channel = channel
                break
        self.is_ready.set()

    async def on_message(self, message):
        if message.channel.parent == self.channel:
            # This is one of our threads
            thread = message.channel
            logger.debug(f"Message: {message!r} ({message.content[:100]})")
            await self.thread_updates.put(await Thread.from_discord(thread))

    async def on_thread_update(self, old_thread, thread):
        logger.debug(f"Update: {thread!r}")
        await self.thread_updates.put(await Thread.from_discord(thread))

    async def get_threads(self):
        await self.is_ready.wait()
        for thread in self.channel.threads:
            yield await Thread.from_discord(thread)

    async def on_raw_message_delete(self, payload):
        thread = await self.fetch_channel(payload.channel_id)
        if thread.parent == self.channel:
            logger.info("Message deleted in our thread, updating")
            await self.thread_updates.put(await Thread.from_discord(thread))

    async def on_raw_message_edit(self, payload):
        thread = await self.fetch_channel(payload.channel_id)
        if thread.parent == self.channel:
            logger.info("Message edited in our thread, updating")
            await self.thread_updates.put(await Thread.from_discord(thread))
