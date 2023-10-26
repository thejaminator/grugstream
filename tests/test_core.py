import datetime
import time
from io import StringIO
from pathlib import Path
from typing import AsyncIterable, Iterable

import anyio
import pytest
from anyio import create_memory_object_stream, create_task_group
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

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
async def test_from_awaitable():
    async def some_awaitable():
        await anyio.sleep(0.1)
        return 1

    observable = Observable.from_awaitable(some_awaitable())
    items = await observable.to_list()
    assert items == [1]


@pytest.mark.asyncio
async def test_flatten_observable_sequential():
    obs1 = Observable.from_iterable([1, 2])
    obs2 = Observable.from_iterable([3, 4])
    outer = Observable.from_iterable([obs1, obs2])
    flattened = outer.flatten_observable_sequential()
    items = await flattened.to_list()
    assert items == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_flatten_observable_sequential_three():
    obs1 = Observable.from_iterable([1, 2])
    obs2 = Observable.from_iterable([3, 4])
    obs3 = Observable.from_iterable([5, 6])
    outer = Observable.from_iterable([obs1, obs2, obs3])
    flattened = outer.flatten_observable_sequential()
    items = await flattened.to_list()
    assert items == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_flatten_observable():
    obs1 = Observable.from_iterable([1, 2])
    obs2 = Observable.from_iterable([3, 4])
    obs3 = Observable.from_iterable([5, 6])
    outer = Observable.from_iterable([obs1, obs2, obs3])
    flattened = outer.flatten_observable()
    items = await flattened.to_list()
    assert items == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_flatten_observable_timed():
    time_start = datetime.datetime.now()
    obs1 = Observable.from_repeat("a", seconds=0.01)
    obs2 = Observable.from_repeat("b", seconds=1)
    outer = Observable.from_iterable([obs1, obs2])
    flattened = outer.flatten_observable().take(15)
    time_end = datetime.datetime.now()
    time_delta = time_end - time_start
    assert time_delta < datetime.timedelta(seconds=0.5)
    items = await flattened.to_list()
    # there should be total 15 items
    assert len(items) == 15
    # 14 items should be "a"
    assert len([item for item in items if item == "a"]) == 14
    # 1 item should be "b"
    assert len([item for item in items if item == "b"]) == 1


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
async def test_map_async_par():
    async def multiply_by_two(x: int) -> int:
        await anyio.sleep(0.1)
        return x * 2

    observable = Observable.from_iterable([1, 2, 3])
    mapped = observable.map_async_par(multiply_by_two)
    items = await mapped.to_list()
    assert items == [2, 4, 6]


@pytest.mark.asyncio
async def test_map_async_par_two_obs():
    async def multiply_by_two(x: int) -> int:
        await anyio.sleep(0.1)
        return x * 2

    observable = Observable.from_iterable([1, 2, 3])
    mapped = observable.map_async_par(multiply_by_two)
    items = await mapped.to_list()
    assert items == [2, 4, 6]

    items_again = await mapped.to_list()
    assert items_again == [2, 4, 6]


@pytest.mark.asyncio
async def test_map_async_par_timed():
    async def multiply_by_two(x: int) -> int:
        await anyio.sleep(0.1)
        return x * 2

    observable = Observable.from_interval(0.01).take(10)
    mapped = observable.map_async_par(multiply_by_two)
    time_start = datetime.datetime.now()
    items = await mapped.to_list()
    assert items == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    time_end = datetime.datetime.now()
    time_delta = time_end - time_start
    assert time_delta < datetime.timedelta(seconds=0.5)


@pytest.mark.asyncio
async def test_map_blocking_par():
    def multiply_by_two(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    observable = Observable.from_interval(0.01).take(10)
    mapped = observable.map_blocking_par(multiply_by_two, max_par=10)
    time_start = datetime.datetime.now()
    items = await mapped.to_list()
    assert items == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    time_end = datetime.datetime.now()
    time_delta = time_end - time_start
    assert time_delta < datetime.timedelta(seconds=0.5)


@pytest.mark.asyncio
async def test_filter():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    filtered = observable.filter(lambda x: x % 2 == 0)
    items = await filtered.to_list()
    assert items == [2, 4]


@pytest.mark.asyncio
async def test_enumerated():
    observable = Observable.from_iterable(["test"] * 5)
    assert await observable.enumerated().to_list() == [
        (0, "test"),
        (1, "test"),
        (2, "test"),
        (3, "test"),
        (4, "test"),
    ]


@pytest.mark.asyncio
async def test_take():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    taken = observable.take(3)
    items = await taken.to_list()
    assert items == [1, 2, 3]


@pytest.mark.asyncio
async def test_take_inclusive():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    taken = observable.take_while_inclusive(lambda x: x < 4)
    items = await taken.to_list()
    assert items == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_take_exclusive():
    observable = Observable.from_iterable([1, 2, 3, 4, 5])
    taken = observable.take_while_exclusive(lambda x: x < 4)
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

    expected_output = ["line1", "line2", "line3"]
    assert items == expected_output


@pytest.mark.asyncio
async def test_from_file_two_obs(tmp_path: Path):
    # Create a test file
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("line1\nline2\nline3\n")

    # Create an observable from the file
    observable = Observable.from_file(file_path)

    # Run the observable and collect the output
    items = await observable.to_list()

    expected_output = ["line1", "line2", "line3"]
    assert items == expected_output

    # Run the observable again
    items_again = await observable.to_list()
    assert items_again == expected_output


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
async def test_for_each_to_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.for_each_to_file(file_path).run_to_completion()

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_opened_file():
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    stringio = StringIO()

    # Write to file
    await observable.to_opened_file(stringio)

    # Check the file contents
    file_contents = stringio.getvalue().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_file_from_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / Path("testfile.txt")

    # Write to file
    await observable.to_file(file_path)

    # Create an observable from the file
    new_observable = Observable.from_file(file_path)
    new_list = await new_observable.to_list()
    assert new_list == test_data


@pytest.mark.asyncio
async def test_run_until_timeout():
    # Create an observable that emits items every 0.01 seconds
    observable = Observable.from_repeat("item", 0.01)

    # Create a list to store received items
    received_items = []
    result = await observable.for_each(lambda item: received_items.append(item)).run_until_timeout(0.3)

    # Check the number of received items. It should be close to 30 (= 0.3 seconds / 0.01 seconds per item)
    # Allowing some leeway for processing time
    assert 25 <= len(received_items) <= 35
    assert result == len(received_items)


@pytest.mark.asyncio
async def test_run_to_completion():
    result: int = await Observable.from_repeat("1", 0.01).take(10).run_to_completion()
    assert result == 10


@pytest.mark.asyncio
async def test_run_to_completion_two_streams():
    obs = Observable.from_repeat("1", 0.01).take(10)
    first_result: int = await obs.run_to_completion()
    second_result: int = await obs.run_to_completion()
    assert first_result == 10
    assert second_result == 10


@pytest.mark.asyncio
async def test_from_list_two_streams():
    obs = Observable.from_iterable([1, 2, 3, 4, 5])
    first_result: int = await obs.run_to_completion()
    second_result: int = await obs.run_to_completion()
    assert first_result == 5
    assert second_result == 5


@pytest.mark.asyncio
async def test_from_generator_two_streams():
    async def gen():
        for i in range(5):
            yield i

    obs = Observable.from_async_iterable(gen())
    first_result: int = await obs.run_to_completion()
    second_result: int = await obs.run_to_completion()
    assert first_result == 5
    # second result should be 0 because the generator is exhausted
    assert second_result == 0


@pytest.mark.asyncio
async def test_from_receive_stream():
    collected = []

    async def process_items(receive_stream: MemoryObjectReceiveStream[str]) -> None:
        obs: Observable[str] = Observable.from_receive_stream(receive_stream)
        await obs.take(5).for_each_to_list(collect_list=collected).print().run_to_completion()

    send_stream, receive_stream = create_memory_object_stream[str](max_buffer_size=1000)
    async with create_task_group() as tg:
        tg.start_soon(process_items, receive_stream)
        async with send_stream:
            for num in range(10):
                await send_stream.send(f'number {num}')
    assert len(collected) == 5


@pytest.mark.asyncio
async def test_for_each_to_stream():
    async def process_items(send_stream: MemoryObjectSendStream[str]) -> None:
        async with send_stream:
            obs: Observable[str] = Observable.from_repeat("a", seconds=0.01)
            await obs.take(5).for_each_to_stream(send_stream).run_to_completion()

    send_stream, receive_stream = create_memory_object_stream[str](max_buffer_size=1000)
    collected = []
    async with create_task_group() as tg:
        tg.start_soon(process_items, send_stream)
        async with receive_stream:
            async for item in receive_stream:
                collected.append(item)

    assert len(collected) == 5


@pytest.mark.asyncio
async def test_receive_and_send_streams():
    collected = []

    async def process_items(
        receive_stream: MemoryObjectReceiveStream[int], send_stream: MemoryObjectSendStream[int]
    ) -> None:
        async with send_stream:
            obs: Observable[int] = Observable.from_receive_stream(receive_stream)
            # send it back
            await (
                # add 1 to demonstrate some processing
                obs.map(lambda x: x + 1)
                .for_each_to_stream(send_stream)
                .take(5)
                .for_each_to_list(collect_list=collected)
                .run_to_completion()
            )

    send_stream, receive_stream = create_memory_object_stream[int](max_buffer_size=1000)
    async with create_task_group() as tg:
        # clone the send stream since we need it for process_items
        cloned_send = send_stream.clone()
        tg.start_soon(process_items, receive_stream, cloned_send)
        async with send_stream:
            # only send 1, but because the observable sends it back to the same stream we should collect 5 items
            await send_stream.send(0)
    assert len(collected) == 5
    assert collected == [1, 2, 3, 4, 5]
