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
        Observable.from_iterable(["one", "two", "three"])
        # this is the same as map, but it's for async functions
        .map_async(lambda item: mock_http_call_to_google(item))
    )

    # Actually start the stream and collect the results into a list
    results = await observable.to_list()

    for response in results:
        print(response)


anyio.run(main)
