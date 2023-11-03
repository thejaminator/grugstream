import asyncio
import random
import string
from pathlib import Path

from tqdm import tqdm

from grugstream import Observable


async def fake_google_search(query: str) -> list[dict[str, str]]:
    """
    Fake Google search that returns a list of results.
    """
    await asyncio.sleep(1)
    return [{"result": f"Result {i} for {query}", "query": query} for i in range(10)]


def generate_random_string():
    # Get all the ASCII letters in lowercase and uppercase
    letters = string.ascii_letters
    # Randomly get a sized 10 string from the letters
    random_string = ''.join(random.choice(letters) for _ in range(10))
    return random_string


def create_big_file(file_name: str):
    with open(file_name, "w") as f:
        for i in range(1000000):
            to_write = generate_random_string()
            f.write(f"{to_write}\n")


async def main():
    file_name = "big_file.txt"
    # Dump a big file, just for example purposes
    create_big_file(file_name)

    observable = (
        # Read the file line by line
        Observable.from_file(Path(file_name))
        # Search google in parallel
        .map_async_par(fake_google_search, max_par=10)
        # Make a tqdm bar
        .tqdm(tqdm_bar=tqdm(desc="Searching Google", unit_scale=1))
        # Since we get a list of results, we want to flatten it
        .flatten_list()
        # We want to stop after 1000 results
        .take(1000)
    )
    # Output the results to a file line by line
    await observable.run_to_file(Path("results.txt"))


if __name__ == "__main__":
    asyncio.run(main())
