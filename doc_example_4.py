import anyio
from tqdm import tqdm

from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(0.1)
    return f"Google Response for {item}"


async def main():
    observable = (
        Observable.from_repeat("query", 0.1)
        .throttle(1)  # don't spam google too much!
        .map_async_par(lambda item: mock_http_call_to_google(item))
        # Show a progress bar that should show ~1 it/s
        .tqdm(tqdm_bar=tqdm(desc="Google observable"))
        # Print the elements
        .print()
    )

    await observable.take(1000).run_to_completion()


anyio.run(main)
