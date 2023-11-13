
import anyio

from grugstream import Observable


async def main():
    # Test to make sure it writes when write_every_n is very high
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]

    def throw() -> None:
        raise ValueError("error")

    observable = (
        Observable.from_iterable(test_data)
        .print()
        .for_each_enumerated(lambda idx, item: None if idx != len(test_data) - 1 else throw())
        .on_error_restart(max_restarts=10)
    )

    # Write to file
    await observable.run_to_completion()


if __name__ == "__main__":
    anyio.run(main)
