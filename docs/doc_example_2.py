import anyio
from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    # Simulate the asynchronous delay of an HTTP request
    await anyio.sleep(1)
    return f"Response from Google {item}"


async def main():
    # Create an observable, and call google for each item
    observable = (
        # repeat every 0.1 seconds
        Observable.from_repeat("one", 0.1)
        # at any given time, there will be at most 50 concurrent calls to google
        .map_async_par(lambda item: mock_http_call_to_google(item), max_par=50)
    )

    # Actually start the stream - results into a list
    # Let's take only 20 results
    results = await observable.take(20).run_to_list()

    for response in results:
        print(response)


anyio.run(main)
