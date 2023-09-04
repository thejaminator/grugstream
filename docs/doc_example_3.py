import random
from pathlib import Path
from typing import List, Optional

import anyio

from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(1)
    return f"Google Response for {item}"


# Mock async function simulating an API call that returns a list of items
async def mock_api_call_that_returns_list(item: str) -> List[str]:
    await anyio.sleep(0.5)
    return [f"Item {i} from {item}" for i in range(3)]


# Mock async function simulating an API call that returns an Optional value
async def mock_api_call_that_returns_optional(item: str) -> Optional[str]:
    await anyio.sleep(0.2)
    maybe_yes = random.choice([True, False])
    return item if maybe_yes else None


async def main():
    observable = (
        Observable.from_repeat("query", 0.1)
        .map_async_par(lambda item: mock_http_call_to_google(item))
        .map_async_par(lambda item: mock_api_call_that_returns_list(item))
        .flatten_iterable()  # Flatten the list into individual items
        .map_async_par(lambda item: mock_api_call_that_returns_optional(item))
        .print()
        .flatten_optional()  # Remove None values
    )

    # Write the results to a file
    await observable.take(100).to_file(Path("results.txt"))


anyio.run(main)
