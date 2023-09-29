from pathlib import Path
from typing import List, Optional
import asyncio

from aiohttp import ClientSession
from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from bs4 import BeautifulSoup

from grugstream import Observable

headers = {
    "User-Agent": "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124"
}


async def fetch_page_content(url: str) -> Optional[str]:
    """
    Fetches the content of the specified URL using aiohttp.
    """
    async with ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            try:
                text = await response.text()
                return text
            # Sometimes we get a UnicodeDecodeError, let's just ignore it
            except UnicodeDecodeError:
                return None


def extract_hyperlinks_from_content(content: str) -> List[str]:
    """
    Extracts all hyperlinks from the given content using BeautifulSoup.
    """
    soup = BeautifulSoup(content, 'html.parser')
    links = [a['href'] for a in soup.find_all('a', href=True)]
    return links


async def get_all_hyperlinks(url: str) -> List[str]:
    """
    Fetches content of the specified URL and extracts all hyperlinks.
    """
    content = await fetch_page_content(url)
    return extract_hyperlinks_from_content(content) if content else []


def is_valid_url(url: str) -> bool:
    return url.startswith("http")


async def main():
    url_to_crawl = "https://webscraper.io/test-sites/e-commerce/allinone"

    async def run_crawler(
        receive_stream: MemoryObjectReceiveStream[str], send_stream: MemoryObjectSendStream[str]
    ) -> None:
        already_seen = set()
        pipeline: Observable[str] = (
            Observable
            # Create an Observable from the receive_stream
            .from_receive_stream(receive_stream)
            .print(prefix="Starting to crawl: ")
            .map_async_par(get_all_hyperlinks)
            # flatten the list of lists into a single list
            .flatten_list()
            # only keep valid urls
            .filter(is_valid_url)
            # only carry on links we haven't seen before
            .filter(lambda link: link not in already_seen)
            # track it so we don't crawl it again
            .for_each(lambda link: already_seen.add(link))
            .print(prefix="Sending new link to crawl ")
            # send it back to the send_stream for processing
            .for_each_to_stream(send_stream)
            # We only want to crawl 1000 links
            .take(1000)
            # output it to a file to save
            .for_each_to_file(file_path=Path("results.txt"))
        )
        await pipeline.run_to_completion()

    send_stream, receive_stream = create_memory_object_stream[str](max_buffer_size=100)
    async with create_task_group() as tg:
        tg.start_soon(run_crawler, receive_stream, send_stream)
        # don't close the send_stream, we want to keep sending items to it
        await send_stream.send(url_to_crawl)


if __name__ == "__main__":
    asyncio.run(main())
