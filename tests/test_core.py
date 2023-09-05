import datetime
from pathlib import Path
from typing import AsyncIterable, Iterable

import anyio
import pytest

from grugstream.core import (
    Observable,
)


@pytest.mark.asyncio
async def test_throttle():
    source_data = [1, 2, 3, 4, 5]

    # Create an observable from the source data
    observable = Observable.from_iterable(source_data)
    # Apply throttling
    throttled_observable = observable.print().throttle(seconds=0.1, max_buffer_size=1)
    time_start = datetime.datetime.now()
    # Run the observable
    items = await throttled_observable.to_list()
    assert items == [1, 2, 3, 4, 5]
    time_end = datetime.datetime.now()
    time_delta = time_end - time_start
    assert time_delta > datetime.timedelta(seconds=0.5)


@pytest.mark.asyncio
async def test_map():
    observable = Observable.from_iterable([1, 2, 3])
    mapped = observable.map(lambda x: x * 2)
    items = await mapped.to_list()
    assert items == [2, 4, 6]


@pytest.mark.asyncio
async def test_map_async():
    async def multiply_by_two(x: int) -> int:
        await anyio.sleep(0.1)
        return x * 2

    observable = Observable.from_iterable([1, 2, 3])
    mapped = observable.map_async(multiply_by_two)
    items = await mapped.to_list()
    assert items == [2, 4, 6]


@pytest.mark.asyncio
async def test_filter():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    filtered = observable.filter(lambda x: x % 2 == 0)
    items = await filtered.to_list()
    assert items == [2, 4]


@pytest.mark.asyncio
async def test_take():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    taken = observable.take(3)
    items = await taken.to_list()
    assert items == [1, 2, 3]


@pytest.mark.asyncio
async def test_distinct():
    observable = Observable.from_iterable([1, 2, 2, 3, 4, 4, 4])
    distinct = observable.distinct()
    items = await distinct.to_list()
    assert items == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_first():
    observable = Observable.from_iterable([1, 2, 3])
    first_item = await observable.first()
    assert first_item == 1


@pytest.mark.asyncio
async def test_flatten_iterable():
    observable = Observable.from_iterable([[1, 2], [3, 4], [5]])
    flattened = observable.flatten_iterable()
    items = await flattened.to_list()
    assert items == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_flatten_async_iterable():
    async def async_gen(items: Iterable[int]) -> AsyncIterable[int]:
        for item in items:
            yield item

    observable = Observable.from_iterable([async_gen([1, 2]), async_gen([3, 4])])
    flattened = observable.flatten_async_iterable()
    items = await flattened.to_list()
    assert items == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_flatten_optional():
    observable = Observable.from_iterable([1, None, 2, 3, None])
    flattened = observable.flatten_optional()
    items = await flattened.to_list()
    assert items == [1, 2, 3]


@pytest.mark.asyncio
async def test_reduce():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    result = await observable.reduce(lambda acc, x: acc + x, 0)
    assert result == 15


@pytest.mark.asyncio
async def test_sum():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    result = await observable.sum()
    assert result == 15


@pytest.mark.asyncio
async def test_from_file(tmp_path: Path):
    # Create a test file
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("line1\nline2\nline3\n")

    # Create an observable from the file
    observable = Observable.from_file(file_path)

    # Run the observable and collect the output
    items = await observable.to_list()

    expected_output = ["line1\n", "line2\n", "line3\n"]
    assert items == expected_output


@pytest.mark.asyncio
async def test_to_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file(file_path)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_run_until_timeout():
    # Create an observable that emits items every 0.1 seconds
    observable = Observable.from_repeat("item", 0.1)

    # Create a list to store received items
    received_items = []
    result = await observable.for_each(lambda item: received_items.append(item)).run_until_timeout(3)

    # Check the number of received items. It should be close to 30 (= 3 seconds / 0.1 seconds per item)
    # Allowing some leeway for processing time
    assert 25 <= len(received_items) <= 35
    assert result == len(received_items)


@pytest.mark.asyncio
async def test_run_to_completion():
    result: int = await Observable.from_repeat("1", 0.01).take(10).run_to_completion()
    assert result == 10
