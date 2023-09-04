import anyio
from pathlib import Path
from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(0.1)
    return f"Google Response for {item}"


async def main():
    my_list = []
    observable = (
        Observable.from_repeat("query", 0.1)
        .map_async_par(lambda item: mock_http_call_to_google(item))
        # What's google's response? Let's write it to a file
        .for_each_to_file(
            file_path=Path("results.txt"),
        )
        # Let's also append it to a list to print
        .for_each(lambda item: my_list.append(item))
        .map(lambda item: item.upper())
        .print()
    )

    await observable.take(10).run_to_completion()
    print(my_list)


anyio.run(main)
