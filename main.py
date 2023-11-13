from pathlib import Path

import anyio

from grugstream import Observable


async def main():
    # Test to make sure it writes when write_every_n is very high
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]

    def throw() -> None:
        raise ValueError("error")

    observable = Observable.from_iterable(test_data).for_each_enumerated(
        lambda idx, item: None if idx != len(test_data) - 1 else throw()
    )

    # Set up the output file path
    file_path = Path("testfile.txt")

    # Write to file
    await observable.to_file_overwriting(file_path, write_every_n=1000)


if __name__ == "__main__":
    anyio.run(main)
