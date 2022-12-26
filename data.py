import datetime
import re
from dataclasses import dataclass
from typing import List

import discord
from discord import Message, Attachment


def slugify(s):
    s = s.lower().strip()
    s = re.sub(r"[^a-z]+", "-", s)
    return s


class Cell:
    def __init__(self, json_data):
        self.json_data = json_data

    @classmethod
    def empty(cls):
        return cls({})

    @classmethod
    def from_string(cls, s):
        return cls({
            "userEnteredValue": {
                "stringValue": s
            }
        })

    @classmethod
    def from_numeric_id(cls, id):
        return cls({
            "userEnteredValue": {
                "stringValue": str(id)
            }
        })

    @classmethod
    def from_formula(cls, formula):
        return cls({
            "userEnteredValue": {
                "formulaValue": formula
            }
        })

    @classmethod
    def from_datetime(cls, dt: datetime.datetime):
        # https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
        # The whole number portion of the value (left of the decimal) counts the days since December 30th 1899.
        # The fractional portion (right of the decimal) counts the time as a fraction of the day.
        since_1899 = dt - datetime.datetime(1899, 12, 30, 0, 0, 0, 0, datetime.timezone.utc)
        serial_number = since_1899.days + (since_1899.seconds / 86400)
        return cls({
            "userEnteredValue": {
                "numberValue": serial_number
            },
            "userEnteredFormat": {
                "numberFormat": {
                    "type": "DATE_TIME"
                }
            },
        })

    @classmethod
    def from_link(cls, link):
        return cls.from_names_and_urls([(link, link)])

    @classmethod
    def from_names_and_urls(cls, names_and_urls):
        text = ""
        text_format_runs = []
        for name, url in names_and_urls:
            text_format_runs.append({
                "startIndex": len(text),
                "format": {
                    "link": {
                        "uri": url,
                    }
                }
            })
            text += name
            text_format_runs.append({
                "startIndex": len(text),
                "format": {}
            })
            text += "\n"
        return cls({
            "userEnteredValue": {
                "stringValue": text[:-1]
            },
            "textFormatRuns": text_format_runs[:-1]
        })


@dataclass
class Thread:
    url: str
    create_date: datetime.datetime
    last_post_date: datetime.datetime
    discord_id: int
    title: str
    poster: str
    messages: List[Message]
    tags: List[str]
    media: List[Attachment]
    save_files: List[Attachment]
    log_files: List[Attachment]
    media: List[Attachment]

    @classmethod
    async def from_discord(cls, discord_thread: discord.Thread) -> "Thread":
        messages = []
        async for message in discord_thread.history(oldest_first=True):
            messages.append(message)

        if messages:
            last_post_date = messages[-1].created_at
            poster = messages[0].author.display_name
        else:
            last_post_date = None
            if discord_thread.owner:
                poster = discord_thread.owner.display_name
            else:
                poster = None

        media = []
        save_files = []
        log_files = []
        for message in messages:
            for attachment in message.attachments:
                if attachment.content_type.startswith(("video", "image")):
                    media.append(attachment)
                elif attachment.filename.endswith("json"):
                    save_files.append(attachment)
                elif attachment.filename.endswith("log"):
                    log_files.append(attachment)

        return cls(
            url=discord_thread.jump_url,
            create_date=discord_thread.created_at,
            last_post_date=last_post_date,
            discord_id=discord_thread.id,
            title=discord_thread.name,
            poster=poster,
            messages=messages,
            tags=[t.name for t in discord_thread.applied_tags],
            media=media,
            save_files=save_files,
            log_files=log_files,
        )

    def main_image(self):
        # Is there a better way to pick out an image to be the "main" one?
        for m in self.media:
            if m.content_type.startswith("image/"):
                return m

    def get_by_header(self, header) -> Cell:
        slug = slugify(header)
        if slug == "discord-id":
            # Don't parse the discord ID pls
            return Cell.from_numeric_id(self.discord_id)
        elif slug == "discord-link":
            return Cell.from_link(self.url)
        elif slug in ("reporter", "poster"):
            return Cell.from_string(self.poster)
        elif slug == "title":
            return Cell.from_string(self.title)
        elif slug in ("details", "message"):
            return Cell.from_string(self.messages[0].content)
        elif slug == "tags":
            return Cell.from_string("\n".join(self.tags))
        elif slug == "save-files":
            return Cell.from_names_and_urls([(att.filename, att.url) for att in self.save_files])
        elif slug == "log-files":
            return Cell.from_names_and_urls([(att.filename, att.url) for att in self.log_files])
        elif slug == "media":
            return Cell.from_names_and_urls([(att.filename, att.url) for att in self.media])
        elif slug == "image":
            im = self.main_image()
            if im:
                return Cell.from_formula(f'=IMAGE("{im.url}")')
        elif slug == "create-date":
            return Cell.from_datetime(self.create_date)
        elif slug == "last-post-date":
            return Cell.from_datetime(self.last_post_date) if self.last_post_date else Cell.empty()
        return Cell.empty()
