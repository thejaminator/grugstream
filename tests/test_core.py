from collections import Counter
import datetime
import time
from pathlib import Path
from typing import AsyncIterable, Iterable, Any

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
async def test_backpressure():
    count = 0

    def increment_count(item: int) -> None:
        nonlocal count
        count += 1

    async def slow_func(item: Any) -> None:
        await anyio.sleep(0.1)

    source_data = Observable.from_interval(seconds=0.01).for_each(increment_count)
    # Run the observable
    await source_data.map_async(slow_func).take(5).run_to_completion()
    assert count <= 6  # shouldn't increment so much
    assert count >= 5


@pytest.mark.asyncio
async def test_backpressure_buffer():
    # No backpressure with a buffer
    count = 0

    def increment_count(item: int) -> None:
        nonlocal count
        count += 1

    async def slow_func(item: Any) -> None:
        await anyio.sleep(0.1)

    source_data = Observable.from_interval(seconds=0.01).for_each(increment_count)
    # Run the observable
    await source_data.buffer_with_size(max_buffer_size=10).map_async(slow_func).take(5).run_to_completion()
    assert count >= 5, "Should create at least 5 items"
    assert count >= 10, "Should create at least 10 items in 0.5 seconds"


@pytest.mark.asyncio
async def test_map():
    observable = Observable.from_iterable([1, 2, 3])
    mapped = observable.map(lambda x: x * 2)
    items = await mapped.to_list()
    assert items == [2, 4, 6]


@pytest.mark.asyncio
async def test_for_each_counter():
    observable = Observable.from_iterable([1, 2, 3])
    counter = Counter()
    mapped = observable.for_each_count(counter)
    items = await mapped.to_list()
    assert items == [1, 2, 3]
    assert counter["count"] == 3


@pytest.mark.asyncio
async def test_for_each_counter_identity():
    observable = Observable.from_iterable([1, 2, 3])
    counter = Counter()
    mapped = observable.for_each_count(counter, key=lambda x: x)
    items = await mapped.to_list()
    assert items == [1, 2, 3]
    assert counter[1] == 1


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
async def test_flatten_observable_no_wait_for_long():
    async def wait_for_1(x: int) -> int:
        await anyio.sleep(0.1)
        return x

    async def wait_for_long(x: int) -> int:
        await anyio.sleep(2)
        return x

    start_time = datetime.datetime.now()
    obs1 = Observable.from_iterable([1, 2, 3, 4]).map_async(wait_for_1)
    obs2 = Observable.from_iterable([10, 20, 30, 40]).map_async(wait_for_long)
    outer = Observable.from_iterable([obs1, obs2])

    flattened = outer.flatten_observable().take(4)

    items = await flattened.to_list()
    end_time = datetime.datetime.now()
    assert items == [1, 2, 3, 4]
    time_delta = end_time - start_time
    assert time_delta < datetime.timedelta(seconds=0.5)


@pytest.mark.asyncio
async def test_merge_observable_no_wait_for_long():
    async def wait_for_1(x: int) -> int:
        await anyio.sleep(0.1)
        return x

    async def wait_for_long(x: int) -> int:
        await anyio.sleep(2)
        return x

    start_time = datetime.datetime.now()
    obs1 = Observable.from_iterable([1, 2, 3, 4]).map_async(wait_for_1)
    obs2 = Observable.from_iterable([10, 20, 30, 40]).map_async(wait_for_long)

    flattened = obs1.merge_with(obs2).take(4)

    items = await flattened.to_list()
    end_time = datetime.datetime.now()
    assert items == [1, 2, 3, 4]
    time_delta = end_time - start_time
    assert time_delta < datetime.timedelta(seconds=0.5)


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
async def test_source_many_subscribes():
    counter = 0

    def count(not_used: Any) -> None:
        nonlocal counter
        counter += 1

    source = Observable.from_iterable([1, 2, 3, 4, 5]).for_each(count)
    await source.map(lambda x: "ok").to_list()
    await source.to_list()
    assert counter == 10


@pytest.mark.asyncio
async def test_map_blocking_par_many():
    def multiply_by_two(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    def multiply_by_4(x: int) -> int:
        time.sleep(0.1)
        return x * 4

    def multiply_by_3(x: int) -> int:
        time.sleep(0.1)
        return x * 3

    observable = Observable.from_interval(0.01).take(10)
    mapped = observable.map_blocking_par(multiply_by_two, max_par=10)
    mapped_again = observable.map_blocking_par(multiply_by_4, max_par=10)
    mapped_3 = observable.map_blocking_par(multiply_by_3, max_par=10)
    items = await mapped.to_list()
    assert items == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    items_again = await mapped_again.to_list()
    assert items_again == [0, 4, 8, 12, 16, 20, 24, 28, 32, 36]
    items_3 = await mapped_3.to_list()
    assert items_3 == [0, 3, 6, 9, 12, 15, 18, 21, 24, 27]


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
async def test_to_file_appending(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file_appending(file_path)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_file_overwriting(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file_overwriting(file_path, write_every_n=10)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_file_overwriting_when_complete(tmp_path: Path):
    # Test to make sure it writes when write_every_n is very high
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file_overwriting(file_path, write_every_n=1000)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_file_overwriting_when_error(tmp_path: Path):
    # Test to make sure it writes when write_every_n is very high
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]

    def throw() -> None:
        raise ValueError("error")

    observable = Observable.from_iterable(test_data).for_each_enumerated(
        lambda idx, item: None if idx != len(test_data) - 1 else throw()
    )

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    with pytest.raises(ValueError):
        # Write to file
        await observable.to_file_overwriting(file_path, write_every_n=1000)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    # should have up til the last item
    without_last = test_data[:-1]
    assert file_contents == without_last


@pytest.mark.asyncio
async def test_to_file_overwriting_twice(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file_overwriting(file_path)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data

    # write again
    await observable.to_file_overwriting(file_path)
    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_on_error_restart(tmp_path: Path):
    counter = 0

    def throw() -> None:
        nonlocal counter
        counter += 1
        raise ValueError("error")

    observable = (
        Observable.from_interval(seconds=0.01)
        .for_each_enumerated(lambda idx, item: throw() if idx >= 10 else None)
        .on_error_restart(max_restarts=5)
    )

    with pytest.raises(ValueError):
        await observable.run_to_completion()
    # assert that we restarted 5 times, this means that the counter should be 6
    assert counter == 6


@pytest.mark.asyncio
async def test_on_error_restart_list():
    counter = 0

    def throw_5_times(item: int) -> None:
        nonlocal counter
        counter += 1
        if counter <= 5:
            raise ValueError("error")

    observable = (
        Observable.from_iterable([1, 2, 3, 4, 5, 6])
        .for_each(lambda x: throw_5_times(x))
        .on_error_restart(max_restarts=5)
    )

    results = await observable.to_list()
    assert results == [1, 2, 3, 4, 5, 6]


@pytest.mark.asyncio
async def test_product():
    obs = Observable.from_iterable([1, 2])
    other = ["a", "b", "c"]
    product = obs.product(other)
    items = await product.to_list()
    assert items == [(1, "a"), (1, "b"), (1, "c"), (2, "a"), (2, "b"), (2, "c")]


@pytest.mark.asyncio
async def test_on_error_restart_async(tmp_path: Path):
    counter = 0

    async def throw() -> None:
        nonlocal counter
        counter += 1
        raise ValueError("error")

    observable = (
        Observable.from_interval(seconds=0.01).map_async_par(lambda x: throw()).on_error_restart(max_restarts=5)
    )

    with pytest.raises(BaseExceptionGroup):
        await observable.run_to_completion()
    # assert that we restarted 5 times, this means that the counter should be 6
    assert counter == 6


@pytest.mark.asyncio
async def test_for_each_to_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.for_each_to_file_appending(file_path).run_to_completion()

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data


@pytest.mark.asyncio
async def test_to_file_from_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / Path("testfile.txt")

    # Write to file
    await observable.to_file_appending(file_path)

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
