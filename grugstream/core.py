from abc import ABC, abstractmethod
from collections import deque
from pathlib import Path
from typing import (
    Callable,
    Awaitable,
    TypeVar,
    Generic,
    AsyncIterable,
    Iterable,
    Sequence,
    Hashable,
    TYPE_CHECKING,
    Protocol,
    Optional,
    Any,
)

import anyio
from anyio import create_task_group, create_memory_object_stream, EndOfStream
from anyio.abc import TaskGroup
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from slist import Slist

from grugstream.acknowledgement import Acknowledgement
from grugstream.exceptions import GrugSumError
from grugstream.subscriber import T_contra, Subscriber, create_subscriber

if TYPE_CHECKING:
    from _typeshed import OpenTextMode

    try:
        from tqdm import tqdm
    except ImportError:
        tqdm = Any
else:
    OpenTextMode = str
    tqdm = Any

A_co = TypeVar("A_co", covariant=True)
A = TypeVar('A')
B = TypeVar("B")
C = TypeVar("C")
B_co = TypeVar("B_co", covariant=True)

CanHash = TypeVar("CanHash", bound=Hashable)


class Addable(Protocol):
    def __add__(self: A, other: A, /) -> A:
        ...


CanAdd = TypeVar("CanAdd", bound=Addable)


def create_observable(subscribe: Callable[["Subscriber[A_co]"], Awaitable[None]]) -> "Observable[A_co]":
    class AnonObservable(Observable[A_co]):  # type: ignore
        async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
            await subscribe(subscriber)

    return AnonObservable()


class Observable(ABC, Generic[A_co]):
    """Abstract base class for Observable."""

    def from_one(self, value: A) -> "Observable[A]":
        return self.from_iterable([value])

    def from_empty(self) -> "Observable[A]":  # type: ignore
        return self.from_iterable([])

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

    @classmethod
    def from_receive_stream(cls, stream: MemoryObjectReceiveStream[A]) -> "Observable[A]":
        async def async_iterator() -> AsyncIterable[A]:
            async with stream:
                async for item in stream:
                    yield item

        return cls.from_async_iterable(async_iterator())

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

    def enumerated(self) -> "Observable[tuple[int, A_co]]":
        source = self

        async def subscribe(subscriber: Subscriber[tuple[int, A_co]]) -> None:
            counter = 0

            async def on_next(value: A_co) -> Acknowledgement:
                nonlocal counter
                idx = counter
                transformed_value = (idx, value)
                counter = counter + 1
                return await subscriber.on_next(transformed_value)

            map_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(map_subscriber)

        return create_observable(subscribe)

    def map(self, func: Callable[[A_co], B_co]) -> "Observable[B_co]":
        source = self

        async def subscribe(subscriber: Subscriber[B_co]) -> None:
            async def on_next(value: A_co) -> Acknowledgement:
                transformed_value = func(value)
                return await subscriber.on_next(transformed_value)

            map_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(map_subscriber)

        return create_observable(subscribe)

    def map_enumerated(self, func: Callable[[int, A_co], B_co]) -> "Observable[B_co]":
        return self.enumerated().map_2(func)

    def map_2(self: "Observable[tuple[A, B]]", func: Callable[[A, B], C]) -> "Observable[C]":
        return self.map(lambda x: func(x[0], x[1]))

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

    def map_2_async(self: "Observable[tuple[A, B]]", func: Callable[[A, B], Awaitable[C]]) -> "Observable[C]":
        return self.map_async(lambda x: func(x[0], x[1]))

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

    def for_each_enumerated(self, func: Callable[[int, A_co], None]) -> "Observable[A_co]":
        def return_original(idx: int, value: A_co) -> A_co:
            func(idx, value)
            return value

        return self.enumerated().map_2(return_original)

    def for_each_to_list(self, collect_list: list[A_co]) -> "Observable[A_co]":
        def append_to_list(value: A_co) -> A_co:
            collect_list.append(value)
            return value

        return self.map(append_to_list)

    def for_each_to_stream(self, stream: MemoryObjectSendStream[A_co]) -> "Observable[A_co]":
        async def send(value: A_co) -> A_co:
            await stream.send(value)
            return value

        return self.map_async(send)

    def for_each_to_file(
        self,
        file_path: Path,
        mode: OpenTextMode = 'a',
        serialize: Callable[[A_co], str] = str,
        write_newline: bool = True,
    ) -> "Observable[A_co]":
        async def append_to_file(value: A_co) -> None:
            async with await anyio.open_file(file_path, mode=mode) as file:
                await file.write(serialize(value) + ('\n' if write_newline else ''))

        return self.for_each_async(append_to_file)

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

    def flatten_optional(self: 'Observable[A | None]') -> 'Observable[A]':
        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A | None) -> Acknowledgement:
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
        send_stream, receive_stream = create_memory_object_stream[A](max_buffer_size=max_buffer_size)  # type: ignore

        class ThrottledObservable(Observable[A]):
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                async def wait_and_forward() -> None:
                    async with create_task_group() as tg:
                        # Producer task
                        tg.start_soon(source.subscribe, send_to_stream_subscriber)

                        # Consumer task
                        tg.start_soon(send_periodically, tg)

                async def send_periodically(tg: TaskGroup) -> None:
                    while True:
                        await anyio.sleep(seconds)
                        try:
                            value = receive_stream.receive_nowait()
                            response = await subscriber.on_next(value)  # type: ignore
                            if response == Acknowledgement.stop:
                                await subscriber.on_completed()
                                tg.cancel_scope.cancel()
                                break
                        except anyio.WouldBlock:
                            # No new elements, keep waiting
                            continue
                        except EndOfStream:
                            await subscriber.on_completed()
                            break

                async def on_next(value: A) -> Acknowledgement:
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

    def tqdm(self, tqdm_bar: Optional[tqdm]) -> 'Observable[A_co]':
        """
        Wrap the observable with a tqdm progress bar.

        :param n: Number of items to expect. If None, we won't log it to tqdm
        :return: Observable with tqdm logging.
        """
        source = self

        try:
            from tqdm import tqdm
        except ImportError:
            raise ImportError("You need to install tqdm to use this function.")

        class TQDMObservable(Observable[A]):
            async def subscribe(self, subscriber) -> None:
                pbar = tqdm(dynamic_ncols=True) if tqdm_bar is None else tqdm_bar  # type: ignore

                async def on_next(value: A) -> Acknowledgement:
                    pbar.update(1)
                    return await subscriber.on_next(value)

                async def on_completed() -> None:
                    pbar.close()
                    await subscriber.on_completed()

                async def on_error(e: Exception) -> None:
                    pbar.close()
                    await subscriber.on_error(e)

                wrapped_subscriber = create_subscriber(on_next=on_next, on_completed=on_completed, on_error=on_error)

                await source.subscribe(wrapped_subscriber)

        return TQDMObservable()

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

        class BufferingSubscriber(Subscriber[A]):
            async def on_next(self, value: A) -> Acknowledgement:
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

    async def to_file(
        self,
        file_path: Path,
        mode: OpenTextMode = 'a',
        serialize: Callable[[A_co], str] = str,
        write_newline: bool = True,
    ) -> None:
        async def on_next(value: A_co) -> Acknowledgement:
            async with await anyio.open_file(file_path, mode=mode) as file:
                await file.write(serialize(value) + ('\n' if write_newline else ''))
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

    async def sum_option(self: "Observable[CanAdd]") -> Optional[CanAdd]:
        """Sums with addition. Returns None if the observable is empty"""
        result = None

        async def on_next(value: CanAdd) -> Acknowledgement:
            nonlocal result
            result = value if result is None else result + value
            return Acknowledgement.ok

        reduce_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(reduce_subscriber)

        return result

    async def sum_or_raise(self: "Observable[CanAdd]") -> CanAdd:
        """Folds left with addition. Raises if the list is empty"""
        result = await self.sum_option()
        if result is None:
            raise GrugSumError("Cannot sum an empty observable")
        return result

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

    def take_while_exclusive(self, predicate: Callable[[A_co], bool]) -> 'Observable[A_co]':
        """Takes elements while the elements fufill the predicate
        If an element does not fulfill the predicate, the stream stops.
        The element that does not fulfill the predicate is not included in the stream"""
        source = self

        async def subscribe_async(subscriber: Subscriber[A_co]) -> None:
            async def on_next(value: A_co) -> Acknowledgement:
                if predicate(value):
                    return await subscriber.on_next(value)
                else:
                    # call on_completed when predicate violated
                    await subscriber.on_completed()
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(on_next=on_next)

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    def take_while_inclusive(self, predicate: Callable[[A_co], bool]) -> 'Observable[A_co]':
        """Takes elements while the elements fufill the predicate
        If an element does not fulfill the predicate, the stream stops.
        The final element that does not fulfill the predicate is included in the stream"""
        source = self

        async def subscribe_async(subscriber: Subscriber[A_co]) -> None:
            async def on_next(value: A_co) -> Acknowledgement:
                if predicate(value):
                    return await subscriber.on_next(value)
                else:
                    # include the violating element in the stream
                    await subscriber.on_next(value)
                    # call on_completed when predicate violated
                    await subscriber.on_completed()
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

    async def run_to_completion(self) -> int:
        """Runs the observable to completion, returning the number of
        final items processed"""
        count = 0

        async def on_next(value: A_co) -> Acknowledgement:
            nonlocal count
            count = count + 1
            return Acknowledgement.ok

        subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(subscriber)

        return count

    async def run_until_timeout(self, seconds: float) -> int:
        """
        Run the observable until a specified timeout (in seconds).

        :param seconds: Time duration to run the observable.

        returns the number of final items processed
        """
        count = 0

        class AnonymousSubscriber(Subscriber[T_contra]):  # type: ignore
            async def on_next(self, value: T_contra) -> Acknowledgement:
                nonlocal count
                count = count + 1
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                task_group.cancel_scope.cancel()

            async def on_completed(self) -> None:
                task_group.cancel_scope.cancel()

        subscriber = AnonymousSubscriber()

        async def timeout_task():
            await anyio.sleep(seconds)
            task_group.cancel_scope.cancel()

        async with create_task_group() as tg:
            task_group = tg  # Set the task_group so we can cancel it in other methods
            tg.start_soon(self.subscribe, subscriber)
            tg.start_soon(timeout_task)

        return count


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
