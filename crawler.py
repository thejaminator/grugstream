import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from typing import List


async def fetch_page_content(url: str) -> str:
    """
    Fetches the content of the specified URL using aiohttp.

    Parameters:
    -----------
    url : str
        The URL to fetch.

    Returns:
    --------
    str
        The content of the fetched URL.
    """
    async with ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()


async def extract_hyperlinks_from_content(content: str) -> List[str]:
    """
    Extracts all hyperlinks from the given content using BeautifulSoup.

    Parameters:
    -----------
    content : str
        The content to extract hyperlinks from.

    Returns:
    --------
    List[str]
        A list of hyperlinks extracted from the content.
    """
    soup = BeautifulSoup(content, 'html.parser')
    links = [a['href'] for a in soup.find_all('a', href=True)]
    # filter to have "strawberry" in the link
    return [link for link in links if "strawberry" in link]


async def get_all_hyperlinks(url: str) -> List[str]:
    """
    Fetches content of the specified URL and extracts all hyperlinks.

    Parameters:
    -----------
    url : str
        The URL to fetch and extract hyperlinks from.

    Returns:
    --------
    List[str]
        A list of hyperlinks extracted from the URL.
    """
    content = await fetch_page_content(url)
    return await extract_hyperlinks_from_content(content)


# Usage example
import asyncio


async def main():
    url_to_crawl = "https://strawberry.rocks"
    hyperlinks = await get_all_hyperlinks(url_to_crawl)
    print(hyperlinks)


if __name__ == "__main__":
    asyncio.run(main())
