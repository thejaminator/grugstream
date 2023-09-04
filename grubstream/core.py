import datetime
from abc import ABC, abstractmethod
from collections import deque
from enum import Enum
from pathlib import Path
from typing import Callable, Awaitable, TypeVar, Generic, AsyncIterable, Iterable, Sequence, Hashable

import anyio
from anyio import run, create_task_group, create_memory_object_stream, EndOfStream
from anyio.abc import TaskGroup
from slist import Slist


class Acknowledgement(str, Enum):
    ok = "ok"
    stop = "stop"


A_co = TypeVar("A_co", covariant=True)
B = TypeVar("B")
B_co = TypeVar("B_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)

A = TypeVar('A')  # New type variable, without covariance

CanHash = TypeVar("CanHash", bound=Hashable)


def create_observable(subscribe: Callable[["Subscriber[A_co]"], Awaitable[None]]) -> "Observable[A_co]":
    class AnonObservable(Observable[A_co]):  # type: ignore
        async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
            await subscribe(subscriber)

    return AnonObservable()


class Observable(ABC, Generic[A_co]):
    """Abstract base class for Observable."""

    def from_one(self, value: A) -> "Observable[A]":
        return self.from_iterable([value])

    def from_one_option(self, value: A | None) -> "Observable[A]":
        return self.from_iterable([value]) if value is not None else self.from_iterable([])

    @staticmethod
    def from_iterable(iterable: Iterable[A]) -> "Observable[A]":
        class IterableObservable(Observable[A]):  # type: ignore
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                ack = Acknowledgement.ok
                for item in iterable:
                    if ack != Acknowledgement.ok:  # If not OK, then stop.
                        break
                    ack = await subscriber.on_next(item)
                await subscriber.on_completed()

        return IterableObservable()

    @staticmethod
    def from_async_iterable(iterable: AsyncIterable[A]) -> "Observable[A]":
        class AsyncIterableObservable(Observable[A]):  # type: ignore
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                ack = Acknowledgement.ok
                async for item in iterable:
                    if ack != Acknowledgement.ok:
                        break
                    ack = await subscriber.on_next(item)
                await subscriber.on_completed()

        return AsyncIterableObservable()

    @staticmethod
    def from_file(file_path: Path) -> "Observable[str]":
        async def async_iterator() -> AsyncIterable[str]:
            async with await anyio.open_file(file_path) as f:
                async for line in f:
                    yield line

        return Observable.from_async_iterable(async_iterator())

    @staticmethod
    def from_interval(seconds: float) -> 'Observable[int]':
        async def emit_values(subscriber: Subscriber[int]) -> None:
            counter = 0
            ack = Acknowledgement.ok
            while ack == Acknowledgement.ok:
                ack = await subscriber.on_next(counter)
                counter += 1
                await anyio.sleep(seconds)

        return create_observable(emit_values)

    @staticmethod
    def from_repeat(
        value: A,
        seconds: float,
    ) -> "Observable[A]":
        async def emit_values(subscriber: Subscriber[A]) -> None:
            ack = Acknowledgement.ok
            while ack == Acknowledgement.ok:
                ack = await subscriber.on_next(value)
                await anyio.sleep(seconds)

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            await emit_values(subscriber)

        return create_observable(subscribe_async)

    @abstractmethod
    async def subscribe(self, subscriber: "Subscriber[A_co]") -> None:
        """Subscribe async subscriber."""
        pass

    def map(self, func: Callable[[A_co], B_co]) -> "Observable[B_co]":
        return MappedObservable(source=self, func=func)

    def map_async(self, func: Callable[[A_co], Awaitable[B_co]]) -> 'Observable[B_co]':
        source = self

        async def subscribe_async(subscriber: Subscriber[B_co]) -> None:
            async def on_next(value: A_co) -> Acknowledgement:
                transformed_value = await func(value)
                return await subscriber.on_next(transformed_value)

            map_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(map_subscriber)

        return create_observable(subscribe_async)

    def map_async_par(
        self, func: Callable[[A_co], Awaitable[B]], max_buffer_size: int = 50, max_par: int = 50
    ) -> 'Observable[B]':
        source = self
        send_stream, receive_stream = create_memory_object_stream[A_co](max_buffer_size=max_buffer_size)

        async def process_with_function(subscriber: Subscriber[B], tg: TaskGroup) -> None:
            semaphore = anyio.Semaphore(max_par)

            async def process_item(item: A_co) -> None:
                async with semaphore:
                    result = await func(item)
                    ack = await subscriber.on_next(result)
                if ack == Acknowledgement.stop:
                    tg.cancel_scope.cancel()

            async for item in receive_stream:
                tg.start_soon(process_item, item)

        async def subscribe_async(subscriber: Subscriber[B]) -> None:
            try:

                async def on_next(value: A_co) -> Acknowledgement:
                    await send_stream.send(value)
                    return Acknowledgement.ok

                async def on_completed() -> None:
                    await send_stream.aclose()

                send_to_stream_subscriber = create_subscriber(on_next=on_next, on_completed=on_completed)

                async with create_task_group() as tg:
                    tg.start_soon(source.subscribe, send_to_stream_subscriber)
                    tg.start_soon(process_with_function, subscriber, tg)
                await subscriber.on_completed()

            except Exception as e:
                await subscriber.on_error(e)
            finally:
                await send_stream.aclose()

        return create_observable(subscribe_async)

    def for_each(self, func: Callable[[A_co], None]) -> "Observable[A_co]":
        def return_original(value: A_co) -> A_co:
            func(value)
            return value

        return self.map(return_original)

    def for_each_async(self, func: Callable[[A_co], Awaitable[None]]) -> "Observable[A_co]":
        async def return_original(value: A_co) -> A_co:
            await func(value)
            return value

        return self.map_async(return_original)

    def filter(self, predicate: Callable[[A_co], bool]) -> "Observable[A_co]":
        return FilteredObservable(source=self, predicate=predicate)

    def distinct(self: 'Observable[CanHash]') -> 'Observable[CanHash]':
        return self.distinct_by(lambda x: x)

    def distinct_by(self: 'Observable[A]', key: Callable[[A], CanHash]) -> 'Observable[A_co]':
        seen = set[CanHash]()

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                hashable_value = key(value)
                if hashable_value not in seen:
                    seen.add(hashable_value)
                    return await subscriber.on_next(value)
                return Acknowledgement.ok

            distinct_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(distinct_subscriber)

        return create_observable(subscribe_async)

    def flatten_iterable(self: 'Observable[Iterable[A]]') -> 'Observable[A]':
        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(iterable: Iterable[A]) -> Acknowledgement:
                for item in iterable:
                    ack = await subscriber.on_next(item)
                    if ack == Acknowledgement.stop:
                        return Acknowledgement.stop
                return Acknowledgement.ok

            flatten_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(flatten_subscriber)

        return create_observable(subscribe_async)

    def flatten_list(self: 'Observable[Sequence[A_co]]') -> 'Observable[A_co]':
        return self.flatten_iterable()

    def flatten_async_iterable(self: 'Observable[AsyncIterable[A]]') -> 'Observable[A]':
        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(iterable: AsyncIterable[A]) -> Acknowledgement:
                async for item in iterable:
                    ack = await subscriber.on_next(item)
                    if ack == Acknowledgement.stop:
                        return Acknowledgement.stop
                return Acknowledgement.ok

            flatten_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(flatten_subscriber)

        return create_observable(subscribe_async)

    def flatten_optional(self: 'Observable[A_co | None]') -> 'Observable[A_co]':
        async def subscribe_async(subscriber: Subscriber[A_co]) -> None:
            async def on_next(value: A_co | None) -> Acknowledgement:
                if value is not None:
                    return await subscriber.on_next(value)
                return Acknowledgement.ok

            flatten_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(flatten_subscriber)

        return create_observable(subscribe_async)

    def flatten_observable(self: 'Observable[Observable[B_co]]') -> 'Observable[B_co]':
        async def subscribe_async(subscriber: Subscriber[B_co]) -> None:
            async def on_inner_next(value: B_co) -> Acknowledgement:
                return await subscriber.on_next(value)

            async def on_next(inner_observable: Observable[B_co]) -> Acknowledgement:
                inner_subscriber = create_subscriber(
                    on_next=on_inner_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
                )
                await inner_observable.subscribe(inner_subscriber)
                return Acknowledgement.ok

            flatten_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(flatten_subscriber)

        return create_observable(subscribe_async)

    def throttle(self, seconds: float, max_buffer_size: int = 1) -> 'Observable[A_co]':
        source = self
        send_stream, receive_stream = create_memory_object_stream[A_co](max_buffer_size=max_buffer_size)

        class ThrottledObservable(Observable[A_co]):
            async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
                async def wait_and_forward() -> None:
                    async with create_task_group() as tg:
                        # Producer task
                        tg.start_soon(source.subscribe, send_to_stream_subscriber)

                        # Consumer task
                        tg.start_soon(send_periodically)

                async def send_periodically() -> None:
                    while True:
                        await anyio.sleep(seconds)
                        try:
                            value = receive_stream.receive_nowait()
                            response = await subscriber.on_next(value)
                            if response == Acknowledgement.stop:
                                await subscriber.on_completed()
                                break
                        except anyio.WouldBlock:
                            # No new elements, keep waiting
                            continue
                        except EndOfStream:
                            await subscriber.on_completed()
                            break

                async def on_next(value: A_co) -> Acknowledgement:
                    await send_stream.send(value)
                    return Acknowledgement.ok

                async def on_completed() -> None:
                    await send_stream.aclose()

                async def on_error(e: Exception) -> None:
                    send_stream.close()
                    await subscriber.on_error(e)

                send_to_stream_subscriber = create_subscriber(
                    on_next=on_next, on_completed=on_completed, on_error=on_error
                )

                await wait_and_forward()

        return ThrottledObservable()

    def print(
        self: "Observable[A_co]", prefix: str = "", printer: Callable[[A_co], None] = print
    ) -> "Observable[A_co]":
        return self.for_each(lambda x: printer(f"{prefix}{x}"))  # type: ignore

    async def to_list(self) -> list[A_co]:
        result = []

        async def on_next(value: A_co) -> Acknowledgement:
            result.append(value)
            return Acknowledgement.ok

        list_subscriber: Subscriber[A_co] = create_subscriber(on_next=on_next)
        await self.subscribe(list_subscriber)

        return result

    async def to_slist(self) -> 'Slist[A_co]':
        return Slist(await self.to_list())

    async def to_set(self) -> set[A_co]:
        result = set()

        async def on_next(value: A_co) -> Acknowledgement:
            result.add(value)
            return Acknowledgement.ok

        set_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(set_subscriber)

        return result

    async def to_async_iterable(self) -> AsyncIterable[A_co]:
        items = []
        event = anyio.Event()
        processing_limit = anyio.CapacityLimiter(1)

        class BufferingSubscriber(Subscriber[A_co]):
            async def on_next(self, value: A_co) -> Acknowledgement:
                await processing_limit.acquire_on_behalf_of(value)
                items.append(value)
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                items.append(error)
                event.set()

            async def on_completed(self) -> None:
                event.set()

        buffering_subscriber = BufferingSubscriber()
        async with anyio.create_task_group() as task_group:

            async def run_subscription():
                try:
                    await self.subscribe(buffering_subscriber)
                finally:
                    event.set()

            task_group.start_soon(run_subscription)

            while not event.is_set() or items:
                await anyio.sleep(0)

                while items:
                    item = items.pop(0)
                    if isinstance(item, Exception):
                        raise item
                    else:
                        yield item
                    processing_limit.release_on_behalf_of(item)

    async def to_file(self: "Observable[str]", file_path: Path, mode: str = 'a') -> None:
        async def on_next(value: A_co) -> Acknowledgement:
            async with await anyio.open_file(file_path, mode=mode) as file:
                await file.write(value + '\n')
            return Acknowledgement.ok

        file_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(file_subscriber)

    async def reduce(self, func: Callable[[A, A], A], initial: A) -> A:
        result = initial

        async def on_next(value: A) -> Acknowledgement:
            nonlocal result
            result = func(result, value)
            return Acknowledgement.ok

        reduce_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(reduce_subscriber)

        return result

    async def sum(self: 'Observable[int | float]') -> int | float:
        return await self.reduce(lambda a, b: a + b, 0)

    def take(self, n: int) -> 'Observable[A_co]':
        source = self

        async def subscribe_async(subscriber: Subscriber[A_co]) -> None:
            count = 0

            async def on_next(value: A_co) -> Acknowledgement:
                nonlocal count
                count += 1
                if count <= n:
                    return await subscriber.on_next(value)
                else:
                    await subscriber.on_completed()  # call on_completed when maximum count is reached
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(on_next=on_next)

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    def take_last(self, n: int) -> 'Observable[A_co]':
        source = self
        buffer = deque(maxlen=n)

        async def subscribe_async(subscriber: Subscriber[A_co]) -> None:
            async def on_next(value: A_co) -> Acknowledgement:
                buffer.append(value)
                return Acknowledgement.ok

            async def on_completed() -> None:
                for item in buffer:
                    await subscriber.on_next(item)
                await subscriber.on_completed()

            take_last_subscriber = create_subscriber(
                on_next=on_next,
                on_completed=on_completed,
                on_error=subscriber.on_error,
            )

            await source.subscribe(take_last_subscriber)

        return create_observable(subscribe_async)

    def limit(self, n: int) -> 'Observable[A_co]':
        # alias for take
        return self.take(n)

    async def first(self) -> A_co:
        items = await self.take(1).to_list()
        return items[0]

    async def run_to_completion(self) -> None:
        await self.subscribe(RunToCompletionSubscriber)


class Subscriber(Generic[T_contra]):
    """Base class for Subscriber.

    From https://monix.io/docs/current/reactive/observable.html
    To obey the contract and preserve back-pressure,
    the Observable will have to wait for its result before it can pass the next element.
    Ack can be either Continue (ok to send the next element) or Stop (we should shut down).
    This way, we can stop the downstream processing (by calling onComplete) and the upstream
     (returning Stop after onNext).
    """

    @abstractmethod
    async def on_next(self, value: T_contra) -> Acknowledgement:
        """Receive next async value."""
        pass

    @abstractmethod
    async def on_error(self, error: Exception) -> None:
        """Receive error."""
        pass

    @abstractmethod
    async def on_completed(self) -> None:
        """Complete async observation."""
        pass


def create_subscriber(
    on_next: Callable[[T_contra], Awaitable[Acknowledgement]] | None = None,
    on_error: Callable[[Exception], Awaitable[None]] | None = None,
    on_completed: Callable[[], Awaitable[None]] | None = None,
) -> Subscriber[T_contra]:
    """Create an Subscriber from functions."""

    class AnonymousSubscriber(Subscriber[T_contra]):  # type: ignore
        async def on_next(self, value: T_contra) -> Acknowledgement:
            if on_next is not None:
                return await on_next(value)
            return Acknowledgement.ok

        async def on_error(self, error: Exception) -> None:
            if on_error is not None:
                await on_error(error)

        async def on_completed(self) -> None:
            if on_completed is not None:
                await on_completed()

    return AnonymousSubscriber()


class FilteredObservable(Observable[A_co]):
    def __init__(self, source: Observable[A_co], predicate: Callable[[A_co], bool]):
        self.source = source
        self.predicate = predicate

    async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
        async def on_next(value: A_co) -> Acknowledgement:
            if self.predicate(value):
                return await subscriber.on_next(value)
            return Acknowledgement.ok

        filter_subscriber = create_subscriber(
            on_next=on_next,
            on_error=subscriber.on_error,
            on_completed=subscriber.on_completed,
        )
        await self.source.subscribe(filter_subscriber)


class PrintSubscriber(Subscriber[T_contra]):
    async def on_next(self, value: T_contra) -> Acknowledgement:
        print(value)
        return Acknowledgement.ok

    async def on_error(self, error: Exception) -> None:
        print(f"Error occurred: {error}")

    async def on_completed(self) -> None:
        print("Completed")


RunToCompletionSubscriber = Subscriber()

# We introduce another TypeVar that can be used in our context
R_co = TypeVar("R_co", covariant=True)


class MappedObservable(Observable[R_co]):
    def __init__(self, source: Observable[A_co], func: Callable[[A_co], R_co]):
        self.source = source
        self.func = func

    async def subscribe(self, subscriber: Subscriber[R_co]) -> None:
        async def on_next(value: A_co) -> Acknowledgement:  # type: ignore
            transformed_value = self.func(value)  # type: ignore
            return await subscriber.on_next(transformed_value)

        map_subscriber: Subscriber[A_co] = create_subscriber(  # type: ignore
            on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
        )

        await self.source.subscribe(map_subscriber)


async def mock_api_call(item: str) -> str:
    await anyio.sleep(0.1)
    return f"Response from {item}"


async def mock_api_call_2(item: str) -> str:
    await anyio.sleep(2)
    return f"Another Response from {item}"


async def main():
    # my_observable: Observable[int] = Observable.from_interval(time_unit=0.01)
    my_observable: Observable[int] = Observable.from_iterable([1, 2, 3, 4])
    stream = (
        my_observable.map(lambda x: x * 2)
        .map(lambda x: f"Number {x}")
        # .throttle(seconds=0.1)
        .map_async_par(mock_api_call, max_par=3)
        .to_async_iterable()
    )

    async for item in stream:
        print(item)


if __name__ == "__main__":
    run(main)
