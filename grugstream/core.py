# import annotations for 3.9 compat
from __future__ import annotations
from abc import ABC, abstractmethod
from collections import deque
import math
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
    Counter,
)
from typing_extensions import deprecated

import anyio
from anyio import create_task_group, create_memory_object_stream, EndOfStream, CapacityLimiter, AsyncFile
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


def create_observable(
    new_subscribe_func: Callable[["Subscriber[T_contra]"], Awaitable[None]]
) -> "Observable[T_contra]":
    """Creates an Observable with a new subscribe function"""

    class AnonObservable(Observable[A]):
        async def subscribe(self, subscriber: Subscriber[A]) -> None:
            await new_subscribe_func(subscriber)

    return AnonObservable()


class Observable(ABC, Generic[A_co]):
    """An asynchronous observable (stream)

    Represents an asynchronous streaming sequence
    """

    def __init__(self) -> None:
        ...
        # self.file_handlers: dict[Path, AsyncFile[str]] = {}

    def from_one(self, value: A) -> "Observable[A]":
        """Create an Observable that emits a single value.

        Parameters
        ----------
        value :
            The value to emit.

        Returns
        -------
        Observable
            An Observable that emits the given value.

        Examples
        --------
        >>> obs = Observable.from_one(10)
        >>> await obs.to_list()
        [10]

        """
        return self.from_iterable([value])

    @classmethod
    def from_awaitable(cls, awaitable: Awaitable[A]) -> "Observable[A]":
        """Create an Observable from an awaitable.

        Parameters
        ----------
        awaitable :
            The awaitable to convert to an Observable.

        Returns
        -------
        Observable
            An Observable emitting the value from the awaitable.

        Examples
        --------
        >>> async def get_value():
        >>>     return 10
        >>> obs = Observable.from_awaitable(get_value())
        >>> await obs.to_list()
        [10]
        """

        async def subscribe(subscriber: Subscriber[A]) -> None:
            value = await awaitable
            await subscriber.on_next(value)
            await subscriber.on_completed()

        return create_observable(subscribe)

    @classmethod
    def from_empty(cls) -> "Observable[A]":  # type: ignore
        """Create an empty Observable that emits no items.

        Returns
        -------
        Observable
            An Observable that emits no items and immediately completes.

        Examples
        --------
        >>> obs = Observable.from_empty()
        >>> await obs.to_list()
        []
        """
        return cls.from_iterable([])

    def from_one_option(self, value: A | None) -> "Observable[A]":
        """Create an Observable emitting value if not None.

        Parameters
        ----------
        value : Any
            The value to emit. If None, emits nothing.

        Returns
        -------
        Observable
            Observable emitting value if not None, otherwise empty.

        Examples
        --------
        >>> obs = Observable.from_one_option(10)
        >>> await obs.to_list()
        [10]

        >>> obs = Observable.from_one_option(None)
        >>> await obs.to_list()
        []
        """
        return self.from_iterable([value]) if value is not None else self.from_iterable([])

    @staticmethod
    def from_iterable(iterable: Iterable[A]) -> "Observable[A]":
        """Create an Observable from an iterable data source.

        Parameters
        ----------
        iterable : Iterable
            The iterable source to convert to an Observable

        Returns
        -------
        Observable
            An Observable emitting the values from the iterable

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.to_list()
        [1, 2, 3]
        """

        class IterableObservable(Observable[B]):
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                ack = Acknowledgement.ok
                for item in iterable:
                    if ack != Acknowledgement.ok:  # If not OK, then stop.
                        break
                    ack = await subscriber.on_next(item)
                await subscriber.on_completed()

        return IterableObservable()

    @staticmethod
    def from_many_observables(*observables: Observable[A_co]) -> "Observable[A_co]":
        """
        Create an Observable from multiple Observables.
        Note that this will try to emit values from the multiple Observables
        concurrently.


        Returns
        -------
        Observable
            Observable emitting all values from nested Observables.

        Examples
        --------
        >>> obs1 = Observable.from_iterable([1, 2])
        >>> obs2 = Observable.from_iterable([3, 4])
        >>> await Observable.from_many_observables(obs1, obs2).to_list()
        [1, 3, 2, 4]
        """
        return Observable.from_iterable(observables).flatten_observable()

    @staticmethod
    def from_iterable_thunk(thunk: Callable[[], Iterable[A]]) -> "Observable[A]":
        """Create an Observable from a thunk that returns an iterable.
        This is useful if you want to re-evaluate the iterable each time.
        For example, generators can only be iterated once, so you can use this to
        re-evaluate the generator each time.

        Parameters
        ----------
        thunk : Callable
            The iterable source to convert to an Observable

        Returns
        -------
        Observable
            An Observable emitting the values from the iterable

        Examples
        --------

        def gen():
            for i in range(3):
                yield i

        >>> obs = Observable.from_iterable_thunk(lambda: [1, 2, 3])
        >>> await obs.to_list()
        [1, 2, 3]
        >>> await obs.to_list() # can be called multiple times, each time it will re-evaluate the thunk
        [1, 2, 3]
        """

        class IterableObservable(Observable[B]):
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                iterable_ = thunk()
                ack = Acknowledgement.ok
                for item in iterable_:
                    if ack != Acknowledgement.ok:  # If not OK, then stop.
                        break
                    ack = await subscriber.on_next(item)
                await subscriber.on_completed()

        return IterableObservable()

    @staticmethod
    def from_async_iterable(iterable: AsyncIterable[A]) -> "Observable[A]":
        """Create an Observable from an asynchronous iterable.

        Parameters
        ----------
        iterable : AsyncIterable
            The asynchronous iterable to convert to an Observable.

        Returns
        -------
        Observable
            An Observable emitting values from the async iterable.

        Examples
        --------
        >>> async def gen():
        >>>     yield 1
        >>>     yield 2
        >>> obs = Observable.from_async_iterable(gen())
        >>> await obs.to_list()
        [1, 2]
        """

        class AsyncIterableObservable(Observable[B]):
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                ack = Acknowledgement.ok
                async for item in iterable:
                    if ack != Acknowledgement.ok:
                        break
                    ack = await subscriber.on_next(item)
                await subscriber.on_completed()

        return AsyncIterableObservable()

    @staticmethod
    def from_async_iterable_thunk(thunk: Callable[[], AsyncIterable[A]]) -> "Observable[A]":
        """Create an Observable from a thunk that returns an iterable.
        This is useful if you want to re-evaluate the iterable each time.
        For example, generators can only be iterated once, so you can use this to
        re-evaluate the generator each time.

        Parameters
        ----------
        thunk : Callable
            The asynchronous iterable to convert to an Observable.

        Returns
        -------
        Observable
            An Observable emitting values from the async iterable.

        Examples
        --------
        >>> async def gen():
        >>>     yield 1
        >>>     yield 2
        >>> obs = Observable.from_async_iterable_thunk(lambda: gen())
        >>> await obs.to_list()
        [1, 2]
        >>> await obs.to_list() # can be called multiple times, each time it will re-evaluate the thunk
        [1, 2]
        """

        class AsyncIterableObservable(Observable[A]):  # type: ignore
            async def subscribe(self, subscriber: Subscriber[A]) -> None:
                generator = thunk()
                ack = Acknowledgement.ok
                async for item in generator:
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
        """Create an Observable that emits lines from a text file.

        Parameters
        ----------
        file_path : Path
            Path to the text file.

        Returns
        -------
        Observable[str]
            An Observable emitting each line of the text file.

        Examples
        --------
        >>> obs = Observable.from_file('data.txt')
        >>> await obs.take(3).to_list()
        ['line1', 'line2', 'line3']
        """

        async def async_iterator() -> AsyncIterable[str]:
            async with await anyio.open_file(file_path) as f:
                async for line in f:
                    line_without_newline = line.rstrip('\n')
                    yield line_without_newline

        return Observable.from_async_iterable_thunk(lambda: async_iterator())

    @staticmethod
    def from_interval(seconds: float) -> 'Observable[int]':
        """Create an Observable emitting incremental numbers periodically.

        Emits an infinite sequence of incremental integers, with time
        period of `seconds` between each emission.

        Parameters
        ----------
        seconds : float
            The interval in seconds between emissions.

        Returns
        -------
        Observable[int]
            An Observable emitting incremental numbers at fixed interval.

        Examples
        --------
        >>> obs = Observable.from_interval(1.0)
        >>> await obs.take(3).to_list()
        [0, 1, 2]
        """

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

    def enumerated(self: Observable[A]) -> "Observable[tuple[int, A]]":
        """Enumerate the values emitted by this Observable.

        Returns
        -------
        Observable
            An Observable of (index, value) tuples.

        Examples
        --------
        >>> obs = Observable.from_iterable(['a', 'b', 'c'])
        >>> enumerated = obs.enumerated()
        >>> await enumerated.to_list()
        [(0, 'a'), (1, 'b'), (2, 'c')]

        """
        source = self

        async def subscribe(subscriber: Subscriber[tuple[int, A]]) -> None:
            counter = 0

            async def on_next(value: A) -> Acknowledgement:
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

    def map(self: Observable[A], func: Callable[[A], B_co]) -> "Observable[B_co]":
        """Map values emitted by this Observable.

        Applies a mapping function to each item emitted by the source.

        Parameters
        ----------
        func : Callable
            The mapping function to apply to each item.

        Returns
        -------
        Observable
            An Observable with the mapped values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> mapped = await obs.map(lambda x: x * 2).to_list()
        >>> mapped
        [2, 4, 6]
        """
        source = self

        async def subscribe(subscriber: Subscriber[B_co]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                try:
                    transformed_value = func(value)
                    return await subscriber.on_next(transformed_value)
                except Exception as e:
                    await subscriber.on_error(e)
                    return Acknowledgement.stop

            map_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(map_subscriber)

        return create_observable(subscribe)

    def map_enumerated(self, func: Callable[[int, A_co], B_co]) -> "Observable[B_co]":
        """Map enumerated values from the Observable.

        Parameters
        ----------
        func : Callable
            The mapping function to apply, taking the index and value.

        Returns
        -------
        Observable
            An Observable with the mapped enumerated values.

        Examples
        --------
        >>> obs = Observable.from_iterable(['a', 'b', 'c'])
        >>> mapped = obs.map_enumerated(lambda i, x: str(i) + x)
        >>> await mapped.to_list()
        ['0a', '1b', '2c']
        """
        return self.enumerated().map_2(func)

    def map_2(self: "Observable[tuple[A, B]]", func: Callable[[A, B], C]) -> "Observable[C]":
        """Map an Observable of pairs using a two-arg function.

        Parameters
        ----------
        func : Callable
            The mapping function taking two arguments.

        Returns
        -------
        Observable
            An Observable with the mapped values.

        Examples
        --------
        >>> obs = Observable.from_iterable([(1, 'a'), (2, 'b')])
        >>> mapped = obs.map_2(lambda x, y: (x, y.upper()))
        >>> await mapped.to_list()
        [(1, 'A'), (2, 'B')]
        """
        return self.map(lambda x: func(x[0], x[1]))

    def map_async(self: Observable[A], func: Callable[[A], Awaitable[B_co]]) -> 'Observable[B_co]':
        """Map values asynchronously using func.

        Parameters
        ----------
        func : Callable
            Async function to apply to each value.

        Returns
        -------
        Observable
            An Observable with the asynchronously mapped values.

        Examples
        --------
        >>> async def double(x):
        >>>     await asyncio.sleep(1)
        >>>     return x * 2
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> mapped = obs.map_async(double)
        >>> await mapped.to_list()
        [2, 4, 6]
        """
        source = self

        async def subscribe_async(subscriber: Subscriber[B_co]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                try:
                    transformed_value = await func(value)
                except Exception as e:
                    await subscriber.on_error(e)
                    return Acknowledgement.stop

                return await subscriber.on_next(transformed_value)

            map_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(map_subscriber)

        return create_observable(subscribe_async)

    def map_2_async(self: "Observable[tuple[A, B]]", func: Callable[[A, B], Awaitable[C]]) -> "Observable[C]":
        """Map pairs asynchronously using func.

        Parameters
        ----------
        func : Callable
            Async function taking two arguments to apply.

        Returns
        -------
        Observable
            An Observable with asynchronously mapped values.

        Examples
        --------
        >>> async def concat(x, y):
        >>>     await asyncio.sleep(1)
        >>>     return f'{x}.{y}'
        >>> obs = Observable.from_iterable([('a', 1), ('b', 2)])
        >>> mapped = obs.map_2_async(concat)
        >>> await mapped.to_list()
        ['a.1', 'b.2']
        """
        return self.map_async(lambda x: func(x[0], x[1]))

    def map_blocking_par(
        self: Observable[A], func: Callable[[A], B_co], max_par: int | CapacityLimiter = 50, max_buffer_size: int = 50
    ) -> 'Observable[B_co]':
        """Map values blocking functions in parallel using func.
        Only use this for IO bound functions - e.g. old code that aren't async functions

        Parameters
        ----------
        func : Callable
            blocking function to apply to each value.
        max_par : int, optional
            Max number of concurrent mappings.
        max_buffer_size : int, optional
            Max size of buffer for pending values.

        Returns
        -------
        Observable
            An Observable with the mapped values.

        Examples
        --------
        >>> def slow_double(x):
        >>>     time.sleep(1)
        >>>     return x * 2
        >>> mapped = Observable.map_blocking_par(slow_double).take(10)
        >>> await mapped.to_list() # runs ~3x faster due to parallel mapping
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        """
        limiter: CapacityLimiter = max_par if isinstance(max_par, CapacityLimiter) else CapacityLimiter(max_par)
        from anyio import to_thread

        async def wrapped_func(value: A) -> B_co:
            return await to_thread.run_sync(func, value, limiter=limiter)

        return self.map_async_par(wrapped_func, max_par=int(limiter.total_tokens), max_buffer_size=max_buffer_size)

    def buffer_with_size(self: Observable[A], max_buffer_size: int | None = 100_000) -> 'Observable[A]':
        """Adds a buffer to the stream

        Parameters
        ----------
        max_buffer_size : int, optional
            Max size of buffer for pending values. If None is passed, an infinite buffer is created.
        """

        async def process_with_function(
            subscriber: Subscriber[A], tg: TaskGroup, receive_stream: MemoryObjectReceiveStream[A]
        ) -> None:
            async def process_item(item: A) -> None:
                ack = await subscriber.on_next(item)
                if ack == Acknowledgement.stop:
                    tg.cancel_scope.cancel()

            async for item in receive_stream:
                tg.start_soon(process_item, item)

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            send_stream, receive_stream = create_memory_object_stream(
                max_buffer_size=max_buffer_size if max_buffer_size is not None else math.inf
            )
            try:

                async def on_next(value: A) -> Acknowledgement:
                    await send_stream.send(value)
                    return Acknowledgement.ok

                async def on_completed() -> None:
                    await send_stream.aclose()

                send_to_stream_subscriber = create_subscriber(
                    on_next=on_next, on_completed=on_completed, on_error=subscriber.on_error
                )

                async with create_task_group() as tg:
                    tg.start_soon(self.subscribe, send_to_stream_subscriber)
                    tg.start_soon(process_with_function, subscriber, tg, receive_stream)
                await subscriber.on_completed()

            except Exception as e:
                await subscriber.on_error(e)
            finally:
                await send_stream.aclose()

        return create_observable(subscribe_async)

    def map_async_par(
        self: Observable[A], func: Callable[[A], Awaitable[B]], max_buffer_size: int | None = 100, max_par: int = 50
    ) -> 'Observable[B]':
        """Map values asynchronously in parallel using func.

        Parameters
        ----------
        func : Callable
            Async function to apply to each value.
        max_buffer_size : int, optional
            Max size of buffer for pending values. If None is passed, an infinite buffer is created.
        max_par : int, optional
            Max number of concurrent mappings.

        Returns
        -------
        Observable
            An Observable with the asynchronously mapped values.

        Examples
        --------
        >>> async def slow_double(x):
        >>>     await asyncio.sleep(1)
        >>>     return x * 2
        >>> source = Observable.interval(0.1).take(10)
        >>> mapped = source.map_async_par(slow_double, max_par=3)
        >>> await mapped.to_list() # runs ~3x faster due to parallel mapping
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        """
        source = self

        async def process_with_function(
            subscriber: Subscriber[B], tg: TaskGroup, receive_stream: MemoryObjectReceiveStream[B]
        ) -> None:
            semaphore = anyio.Semaphore(max_par)

            async def process_item(item: A) -> None:
                async with semaphore:
                    try:
                        result = await func(item)
                    except Exception as e:
                        await subscriber.on_error(e)
                        tg.cancel_scope.cancel()
                    ack = await subscriber.on_next(result)  # type: ignore
                if ack == Acknowledgement.stop:
                    tg.cancel_scope.cancel()

            async for item in receive_stream:
                tg.start_soon(process_item, item)

        async def subscribe_async(subscriber: Subscriber[B]) -> None:
            send_stream, receive_stream = create_memory_object_stream(
                max_buffer_size=max_buffer_size if max_buffer_size is not None else math.inf
            )
            try:

                async def on_next(value: A) -> Acknowledgement:
                    await send_stream.send(value)
                    return Acknowledgement.ok

                async def on_completed() -> None:
                    await send_stream.aclose()

                send_to_stream_subscriber = create_subscriber(
                    on_next=on_next, on_completed=on_completed, on_error=subscriber.on_error
                )

                async with create_task_group() as tg:
                    tg.start_soon(source.subscribe, send_to_stream_subscriber)
                    tg.start_soon(process_with_function, subscriber, tg, receive_stream)
                await subscriber.on_completed()

            except Exception as e:
                await subscriber.on_error(e)
            finally:
                await send_stream.aclose()

        return create_observable(subscribe_async)

    def for_each_count(
        self: Observable[A], counter: Counter[Any], key: Callable[[A], CanHash] = lambda x: "count"
    ) -> "Observable[A]":
        """Increment counter for each value.

        Parameters
        ----------
        counter : Counter
            The counter to increment.
        key : Callable, optional
            Function to get the key to increment, by default lambda x: x['count']

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> counter = Counter()
        >>> obs = Observable.from_iterable([1,2,3])
        >>> obs.for_each_count(counter).run_to_completion()
        >>> counter
        Counter({"count": 1})
        """

        def counter_update(ele: A):
            counter_key = key(ele)
            counter[counter_key] += 1

        return self.for_each(counter_update)

    def for_each(self: Observable[A], func: Callable[[A], Any]) -> "Observable[A]":
        """Apply func to each value but don't modify stream.

        Parameters
        ----------
        func : Callable
            Function to apply to each value.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> obs.for_each(print).to_list()
        1
        2
        3
        [1, 2, 3]
        """

        def return_original(value: A) -> A:
            func(value)
            return value

        return self.map(return_original)

    def for_each_enumerated(self: Observable[A], func: Callable[[int, A], Any]) -> "Observable[A]":
        """Apply indexed func to each value, but don't modify stream.

        Parameters
        ----------
        func : Callable
            Function accepting index and value.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> obs = Observable.from_iterable(['a', 'b', 'c'])
        >>> obs.for_each_enumerated(lambda i, x: print(f'{i}: {x}')).to_list()
        0: a
        1: b
        2: c
        ['a', 'b', 'c']
        """

        def return_original(idx: int, value: A) -> A:
            func(idx, value)
            return value

        return self.enumerated().map_2(return_original)

    def for_each_to_list(self: Observable[A], collect_list: list[A]) -> "Observable[A]":
        """Append each value to a list.

        Parameters
        ----------
        collect_list : list
            The list to append values to.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> my_list = []
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> obs.for_each_to_list(my_list).to_list()
        [1, 2, 3]
        >>> my_list
        [1, 2, 3]
        """

        def append_to_list(value: A) -> A:
            collect_list.append(value)
            return value

        return self.map(append_to_list)

    def for_each_to_stream(self: Observable[A], stream: MemoryObjectSendStream[A]) -> "Observable[A]":
        """Send each value to a stream.

        Parameters
        ----------
        stream : MemoryObjectSendStream
            The stream to send values to.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> send_stream, _ = create_memory_object_stream()
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> obs.for_each_to_stream(send_stream)
        >>> # `send_stream` will have received the values
        """

        async def send(value: A) -> A:
            await stream.send(value)
            return value

        return self.map_async(send)

    def on_error_restart(
        self: Observable[A_co],
        max_restarts: int | None = 1000,
        exceptions: tuple[type[Exception]] = (Exception,),
        log_restarting_func: Callable[[int, Exception], None]
        | None = lambda restart_count, exception: print(
            f"Encountered {exception}, restarting with try {restart_count}"
        ),
        log_unhandled_func: Callable[[int, Exception], None]
        | None = lambda restart_count, exception: print(
            f"Encountered unhandled {exception}, total restarts so far: {restart_count}"
        ),
    ) -> "Observable[A_co]":
        """Restarts the observable if the exceptions are encountered"""
        source: Observable[A_co] = self
        count = 0
        if max_restarts is not None:
            assert max_restarts > 0, "max_restarts must be more than 0"
        max_restarts_num = max_restarts if max_restarts is not None else math.inf

        async def subscribe(subscriber: Subscriber[A_co]) -> None:
            class RestartSubscriber(Subscriber[B]):
                def __init__(self) -> None:
                    self.__threshold_reached: bool = False

                async def on_error(self, error: Exception) -> None:
                    if isinstance(error, exceptions):
                        nonlocal count
                        count += 1
                        if count <= max_restarts_num:
                            if log_restarting_func:
                                log_restarting_func(count, error)
                            # restart
                            # TODO: Trampoline to avoid infinite recursive blowup?
                            restarted_subscriber = RestartSubscriber()
                            await source.subscribe(restarted_subscriber)
                            return None
                    if log_unhandled_func:
                        log_unhandled_func(count, error)
                    # Raise if max restart reached or not caught
                    self.__threshold_reached = True
                    raise error

                async def on_next(self, value: B) -> Acknowledgement:
                    if self.__threshold_reached:
                        return Acknowledgement.stop
                    else:
                        return await subscriber.on_next(value)  # type: ignore

            subscriber_with_on_error = RestartSubscriber()

            await source.subscribe(subscriber_with_on_error)

        return create_observable(subscribe)

    @deprecated("Use for_each_to_file_appending instead, mode has been remove")
    def for_each_to_file(
        self,
        file_path: Path,
        mode: OpenTextMode = 'a',
        serialize: Callable[[A_co], str] = str,
        write_newline: bool = True,
    ) -> "Observable[A_co]":
        return self.for_each_to_file_appending(file_path, serialize, write_newline)

    def for_each_to_file_appending(
        self: Observable[A],
        file_path: Path,
        serialize: Callable[[A], str] = str,
        write_newline: bool = True,
    ) -> "Observable[A]":
        """
        Pass through and appends to a file continuously

        Parameters
        ----------
        file_path : Path
            Path to write the file to.
        serialize : Callable, default str
            Function to serialize values to strings.
        write_newline : bool, default True
            Whether to write a newline after each value.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.for_each_to_file(Path('data.txt')).run_to_completion()
        # data.txt will contain '1\n2\n3\n'
        """

        # need a lock to prevent multiple awaitable when it isn't ok to write yet
        can_write = anyio.Semaphore(1)
        source = self

        async def next_subscriber(subscriber: Subscriber[A]) -> None:
            class AnonSubscriber(Subscriber[Any]):
                def __init__(self) -> None:
                    self.file_handlers: dict[Path, AsyncFile[str]] = {}

                async def on_next(self, value: A) -> Acknowledgement:
                    if file_path not in self.file_handlers:
                        file_path.parent.mkdir(exist_ok=True, parents=True)
                        file = await anyio.open_file(file_path, mode="a")
                        self.file_handlers[file_path] = file
                    else:
                        file = self.file_handlers[file_path]
                    async with can_write:
                        write_str = serialize(value) + ('\n' if write_newline else '')
                        await file.write(write_str)

                    return await subscriber.on_next(value)

                async def on_error(self, error: Exception) -> None:
                    file = self.file_handlers.get(file_path)
                    if file is not None:
                        await file.aclose()
                    return await subscriber.on_error(error)

                async def on_completed(self) -> None:
                    file = self.file_handlers.get(file_path)
                    if file is not None:
                        await file.aclose()
                    return await subscriber.on_completed()

            await source.subscribe(AnonSubscriber())

        return create_observable(next_subscriber)

    def for_each_async(self: Observable[A], func: Callable[[A], Awaitable[None]]) -> "Observable[A]":
        """Apply asynchronous func to each value.

        Parameters
        ----------
        func : Callable
            Asynchronous function to apply.

        Returns
        -------
        Observable
            Output Observable with values unchanged.

        Examples
        --------
        >>> async def print_delayed(x):
        >>>     await asyncio.sleep(1)
        >>>     print(x)
        >>>
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> obs.for_each_async(print_delayed).to_list()
        1    # printed after 1 second
        2    # printed after 1 more second
        3
        [1, 2, 3]
        """

        async def return_original(value: A) -> A:
            await func(value)
            return value

        return self.map_async(return_original)

    def filter(self: Observable[A], predicate: Callable[[A], bool]) -> "Observable[A_co]":
        """Filter values emitted by this Observable.

        Parameters
        ----------
        predicate : callable
            The function to evaluate for each item.

        Returns
        -------
        Observable
            An Observable only emitting values where predicate is True.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3, 4])
        >>> filtered = obs.filter(lambda x: x % 2 == 0)
        >>> await filtered.to_list()
        [2, 4]

        """

        async def new_subsribe_func(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                try:
                    if predicate(value):
                        return await subscriber.on_next(value)
                except Exception as e:
                    await subscriber.on_error(e)
                    return Acknowledgement.stop
                return Acknowledgement.ok

            filter_subscriber = create_subscriber(
                on_next=on_next,
                on_error=subscriber.on_error,
                on_completed=subscriber.on_completed,
            )
            await self.subscribe(filter_subscriber)

        return create_observable(new_subsribe_func)

    def distinct(self: 'Observable[CanHash]') -> 'Observable[CanHash]':
        """Filter Observable to only emit distinct values.

        Items are compared directly for uniqueness.
        Note that this requires items to implement __hash__.
        This uses a set to track seen hashes, so it will use O(n) memory,
        but should not be that much since it only stores hashes.

        Returns
        -------
        Observable
            Observable that contains items that implement __hash__.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 2, 3, 3, 1])
        >>> distinct = obs.distinct()
        >>> await distinct.to_list()
        [1, 2, 3]
        """
        return self.distinct_by(lambda x: x)

    def distinct_by(self: 'Observable[A]', key: Callable[[A], CanHash]) -> 'Observable[A_co]':
        """Filter Observable to only emit values with distinct keys.

        Items with different keys are considered distinct.
        This uses a set to track seen hashes, so it will use O(n) memory,
        but should not be that much since it only stores hashes.

        Parameters
        ----------
        key : Callable
            Function to extract comparison key for each item.

        Returns
        -------
        Observable
            Observable of items with distinct keys.

        Examples
        --------
        >>> obs = Observable.from_iterable([{'id': 1}, {'id': 2}, {'id': 1}])
        >>> distinct = obs.distinct_by(lambda x: x['id'])
        >>> await distinct.to_list()
        [{'id': 1}, {'id': 2}]
        """
        seen = set[CanHash]()

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                try:
                    hashable_value = key(value)
                    if hashable_value not in seen:
                        seen.add(hashable_value)
                        return await subscriber.on_next(value)
                    return Acknowledgement.ok
                except Exception as e:
                    await subscriber.on_error(e)
                    return Acknowledgement.stop

            distinct_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await self.subscribe(distinct_subscriber)

        return create_observable(subscribe_async)

    def flatten_iterable(self: 'Observable[Iterable[A]]') -> 'Observable[A]':
        """Flatten an Observable of iterables into an Observable of values.

        Flattens an Observable of nested iterables into a single
        Observable emitting all the nested values.

        Returns
        -------
        Observable[A]
            Observable emitting all values from nested iterables.

        Examples
        --------
        >>> obs = Observable.from_iterable([[1, 2], [3, 4]])
        >>> flattened = obs.flatten_iterable()
        >>> await flattened.to_list()
        [1, 2, 3, 4]
        """

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

    def flatten_list(self: 'Observable[Sequence[A]]') -> 'Observable[A]':
        """Flatten an Observable of lists into an Observable of values."""
        return self.flatten_iterable()

    def flatten_async_iterable(self: 'Observable[AsyncIterable[A]]') -> 'Observable[A]':
        """Flatten an Observable of async iterables into an Observable of values.

        Flattens an Observable of nested async iterables into a single
        Observable emitting all the nested values.

        Returns
        -------
        Observable[A]
            Observable emitting all values from nested async iterables.

        Examples
        --------
        >>> async def gen(x):
        >>>     yield x
        >>> obs = Observable.from_iterable([gen(1), gen(2)])
        >>> flattened = obs.flatten_async_iterable()
        >>> await flattened.to_list()
        [1, 2]
        """

        return self.map(Observable.from_async_iterable).flatten_observable()

    def flatten_optional(self: 'Observable[A | None]') -> 'Observable[A]':
        """Flatten an Observable of Optional values into an Observable of present values.

        Flattens an Observable of Optional values, removing any None values.

        Returns
        -------
        Observable[A]
            Observable only emitting present values, removing any Nones.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, None, 2, None, 3])
        >>> flattened = obs.flatten_optional()
        >>> await flattened.to_list()
        [1, 2, 3]
        """

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

    def flatten_observable_sequential(self: 'Observable[Observable[B]]') -> 'Observable[B]':
        """Flatten Observable of Observables into one Observable.

        Flattens an Observable emitting other Observables, into a single
        Observable emitting all the values from the nested Observables.
        Note that this outputs values from the nested Observables in
        sequential order.

        Returns
        -------
        Observable[B]
            Observable emitting all values from nested Observables.

        Examples
        --------
        >>> obs1 = Observable.from_iterable([1, 2])
        >>> obs2 = Observable.from_iterable([3, 4])
        >>> outer = Observable.from_iterable([obs1, obs2])
        >>> flattened = outer.flatten_observable()
        >>> await flattened.to_list()
        [1, 2, 3, 4]
        """

        async def subscribe_async(subscriber: Subscriber[B]) -> None:
            async def on_inner_next(value: B) -> Acknowledgement:
                return await subscriber.on_next(value)

            async def on_next(inner_observable: Observable[B]) -> Acknowledgement:
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

    def flatten_observable(self: 'Observable[Observable[B]]') -> 'Observable[B]':
        """Flatten Observable of Observables into one Observable.

        Flattens an Observable emitting other Observables, into a single
        Observable emitting all the values from the nested Observables.
        Note that this will try to emit values from the nested Observables
        concurrently.


        Returns
        -------
        Observable[B]
            Observable emitting all values from nested Observables.

        Examples
        --------
        >>> obs1 = Observable.from_iterable([1, 2])
        >>> obs2 = Observable.from_iterable([3, 4])
        >>> outer = Observable.from_iterable([obs1, obs2])
        >>> flattened = outer.flatten_observable()
        >>> await flattened.to_list()
        [1, 3, 4, 2]
        """

        async def subscribe_async(subscriber: Subscriber[B]) -> None:
            async def subscribe_inner(inner_observable: Observable[B]) -> None:
                async def on_next(value: B) -> Acknowledgement:
                    ack = await subscriber.on_next(value)
                    if ack == Acknowledgement.stop:
                        tg.cancel_scope.cancel()
                    return ack

                async def on_error(e: Exception) -> None:
                    tg.cancel_scope.cancel()
                    await subscriber.on_error(e)

                async def on_completed() -> None:
                    await subscriber.on_completed()

                await inner_observable.subscribe(
                    create_subscriber(on_next=on_next, on_error=on_error, on_completed=on_completed)
                )

            async with anyio.create_task_group() as tg:
                async for inner_observable in self.to_async_iterable():
                    tg.start_soon(subscribe_inner, inner_observable)

        return create_observable(subscribe_async)

    def merge_with(self: 'Observable[A_co]', *others: 'Observable[A_co]') -> 'Observable[A_co]':
        """Merge this Observable with other Observables.

        Parameters
        ----------
        others : Observable
            Other Observables to merge with.
        Returns
        -------
        Observable
            Observable emitting values from this and others Observables. Note that this
            will not preserve order between Observables.

        Examples
        --------
        >>> obs1 = Observable.from_iterable([1, 2])
        >>> obs2 = Observable.from_iterable([3, 4])
        >>> merged = obs1.merge_with(obs2)
        >>> await merged.to_list()
        [1, 3, 4, 2]
        """
        new = self.from_iterable([self, *others])
        return new.flatten_observable()

    def throttle(self, seconds: float, max_buffer_size: int = 1) -> 'Observable[A_co]':
        """Throttle emissions to at most one per `seconds` interval.

        Parameters
        ----------
        seconds : float
            Interval duration between emissions
        max_buffer_size : int, default 1
            Max number of values to buffer

        Returns
        -------
        Observable
            Throttled Observatory allowing at most one emission per interval

        Examples
        --------
        >>> obs = Observable.interval(0.1)
        >>> throttled = obs.throttle(1.0)
        >>> await throttled.take(3).to_list()
        [0, 1, 2] # emitted at 1 second intervals
        """
        source = self
        send_stream, receive_stream = create_memory_object_stream(max_buffer_size=max_buffer_size)  # type: ignore

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
        """Print values from the Observable using print().

        Prints each value emitted by the Observable after prepending
        `prefix`.

        Parameters
        ----------
        prefix : str, default ""
            String to prepend to printed values.
        printer : Callable, default print
            Function to use for printing.

        Returns
        -------
        Observable
            Output Observable with unchanged values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> obs.print("Item: ").to_list()
        Item: 1
        Item: 2
        Item: 3
        [1, 2, 3]
        """
        return self.for_each(lambda x: printer(f"{prefix}{x}"))  # type: ignore

    def tqdm(self, tqdm_bar: Optional[tqdm] = None) -> 'Observable[A_co]':  # type: ignore
        """Wrap the Observable with a tqdm progress bar.

        Parameters
        ----------
        tqdm_bar : Optional[tqdm], default None
            tqdm progress bar to use, or None to use default

        Returns
        -------
        Observable
            Wrapped Observatory with tqdm progress bar

        Examples
        --------
        >>> from tqdm import tqdm
        >>> pbar = tqdm(desc="Progress")
        >>> obs = Observable.from_interval(1)
        >>> obs.tqdm(pbar).take(10).run_to_completion()
        # pbar will show 1 it/s progress
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

    async def to_list(self: Observable[A]) -> list[A]:
        """Collect all values from the Observable into a list.

        Returns
        -------
        list
            List containing all values emitted by the Observable.

        Examples
        --------
        >>> obs = Observable.from_interval(0.1).take(3)
        >>> await obs.to_list()
        [1, 2, 3]
        """
        result = []

        async def on_next(value: A) -> Acknowledgement:
            result.append(value)
            return Acknowledgement.ok

        list_subscriber: Subscriber[A] = create_subscriber(
            on_next=on_next,
            on_error=None,
            on_completed=None,
        )
        await self.subscribe(list_subscriber)

        return result

    async def to_slist(self) -> 'Slist[A_co]':
        """Collect values into an Slist.

        Returns
        -------
        Slist
            Slist containing all values emitted.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.to_slist()
        Slist([1, 2, 3])
        """
        return Slist(await self.to_list())

    async def to_set(self: "Observable[CanHash]") -> set[CanHash]:
        """Collect values into a set, removing duplicates.

        Returns
        -------
        set
            Set containing unique values emitted.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 1])
        >>> await obs.to_set()
        {1, 2}
        """
        result = set()

        async def on_next(value: CanHash) -> Acknowledgement:
            result.add(value)
            return Acknowledgement.ok

        set_subscriber = create_subscriber(on_next=on_next, on_completed=None, on_error=None)

        await self.subscribe(set_subscriber)

        return result

    async def to_async_iterable(self) -> AsyncIterable[A_co]:
        """Collect values into an async iterable.

        Returns
        -------
        AsyncIterable[A_co]
            Async iterable containing all emitted values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> async for value in obs.to_async_iterable():
        >>>     print(value)
        1
        2
        3
        """
        items = []
        event = anyio.Event()
        processing_limit = anyio.CapacityLimiter(1)

        class BufferingSubscriber(Subscriber[A]):
            async def on_next(self, value: A) -> Acknowledgement:
                await processing_limit.acquire_on_behalf_of(value)
                items.append(value)
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                event.set()
                raise error

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

    @deprecated("Use to_file_appending or to_file_overwriting instead, mode has been removed")
    async def to_file(
        self,
        file_path: Path,
        mode: OpenTextMode = 'a',
        serialize: Callable[[A_co], str] = str,
        write_newline: bool = True,
    ) -> None:
        await self.to_file_appending(file_path, serialize, write_newline)

    async def to_file_appending(
        self: Observable[A],
        file_path: Path,
        serialize: Callable[[A], str] = str,
        write_newline: bool = True,
    ) -> None:
        """Write all emitted values to a file, by appending.
        Note that this appends to a file, rather than overwriting it.

        Parameters
        ----------
        file_path : Path
            Path to write output file to.
        serialize : Callable, default str
            Function to serialize items to strings.
        write_newline : bool, default True
            Whether to write newline after each value.
        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.to_file('data.txt')
        """

        # lock to prevent multiple awaitables from writing at the same time
        can_write = anyio.Semaphore(1)

        class AnonymousSubscriber(Subscriber[Any]):
            def __init__(self) -> None:
                self.file_handlers: dict[Path, AsyncFile[str]] = {}

            async def on_next(self, value: A) -> Acknowledgement:
                # Only open file ONCE when first value is received
                if file_path not in self.file_handlers:
                    file_path.touch(exist_ok=True)
                    file = await anyio.open_file(file_path, mode="a")
                    self.file_handlers[file_path] = file
                else:
                    file = self.file_handlers[file_path]
                async with can_write:
                    await file.write(serialize(value) + ('\n' if write_newline else ''))
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                file = self.file_handlers.get(file_path)
                if file is not None:
                    await file.aclose()
                raise error

            async def on_completed(self) -> None:
                file = self.file_handlers.get(file_path)
                if file is not None:
                    await file.aclose()
                return None

        new_subscriber = AnonymousSubscriber()

        await self.subscribe(new_subscriber)

    async def to_file_overwriting(
        self: Observable[A],
        file_path: Path,
        serialize: Callable[[A], str] = str,
        write_newline: bool = True,
        write_every_n: int = 100,
    ) -> None:
        """Write all emitted values to a file, by overwriting the current file.
        Note that this stores values to a buffer, so this can lead to an OOM in large files.
        We recommend to use to_file_appending instead if memory is a concern

        Parameters
        ----------
        file_path : Path
            Path to write output file to.
        serialize : Callable, default str
            Function to serialize items to strings.
        write_newline : bool, default True
            Whether to write newline after each value.
        write_every_n : int, default 200
            Only writes to the file every n values. A higher value prevents your stream from slowing
            down due to slow write times.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.to_file('data.txt')
        """

        # lock to prevent multiple awaitables from writing at the same time
        can_write = anyio.Semaphore(1)
        buffer: list[str] = []

        class AnonymousSubscriber(Subscriber[Any]):
            def __init__(self) -> None:
                self.file_handlers: dict[Path, AsyncFile[str]] = {}

            async def on_next(self, value: A) -> Acknowledgement:
                # Only open file ONCE when first value is received
                if file_path not in self.file_handlers:
                    # First time
                    file_path.parent.touch(exist_ok=True)
                    file = await anyio.open_file(file_path, mode="w")
                    self.file_handlers[file_path] = file
                else:
                    file = self.file_handlers[file_path]
                async with can_write:
                    buffer.append(serialize(value) + ('\n' if write_newline else ''))
                    if len(buffer) == write_every_n:
                        await file.writelines(buffer)
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                file = self.file_handlers.get(file_path)
                if file is not None:
                    async with can_write:
                        # Write the buffer
                        await file.writelines(buffer)
                        await file.aclose()
                raise error

            async def on_completed(self) -> None:
                file = self.file_handlers.get(file_path)
                if file is not None:
                    async with can_write:
                        # Write the buffer
                        await file.writelines(buffer)
                        await file.aclose()
                return None

        new_subscriber = AnonymousSubscriber()

        await self.subscribe(new_subscriber)

    async def reduce(self, func: Callable[[A, A], A], initial: A) -> A:
        """Reduce the Observable using `func`, starting with `initial`.

        Parameters
        ----------
        func : Callable[[A, A], A]
            Function to apply to accumulate values.
        initial : A
            Initial value to start reduction from.

        Returns
        -------
        A
            Final accumulated reduction value.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.reduce(lambda acc, x: acc + x, 0)
        6
        """
        result = initial

        async def on_next(value: A) -> Acknowledgement:
            nonlocal result
            result = func(result, value)
            return Acknowledgement.ok

        reduce_subscriber = create_subscriber(on_next=on_next, on_completed=None, on_error=None)

        await self.subscribe(reduce_subscriber)

        return result

    async def sum(self: 'Observable[int | float]') -> int | float:
        """Sum all emitted values.

        Returns
        -------
        int | float
            Sum of all emitted values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.sum()
        6
        """
        return await self.reduce(lambda a, b: a + b, 0)

    async def sum_option(self: "Observable[CanAdd]") -> Optional[CanAdd]:
        """Sum values using +, return None if empty.

        Returns
        -------
        Optional[CanAdd]
            Sum of values or None if empty.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.sum_option()
        6
        >>> empty = Observable.from_iterable([])
        >>> await empty.sum_option()
        None
        """
        result = None

        async def on_next(value: CanAdd) -> Acknowledgement:
            nonlocal result
            result = value if result is None else result + value
            return Acknowledgement.ok

        reduce_subscriber = create_subscriber(on_next=on_next, on_completed=None, on_error=None)

        await self.subscribe(reduce_subscriber)

        return result

    async def sum_or_raise(self: "Observable[CanAdd]") -> CanAdd:
        """Sum values using +, raise if empty.

        Raises
        ------
        GrugSumError
            If the Observable is empty.

        Returns
        -------
        CanAdd
            Sum of values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.sum_or_raise()
        6
        >>> empty = Observable.from_iterable([])
        >>> await empty.sum_or_raise()
        # raises GrugSumError
        """
        result = await self.sum_option()
        if result is None:
            raise GrugSumError("Cannot sum an empty observable")
        return result

    def take(self: Observable[A], n: int) -> 'Observable[A]':
        """Take the first n values from the Observable.

        Parameters
        ----------
        n : int
            Number of values to take.

        Returns
        -------
        Observable
            Observable emitting the first n values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3, 4, 5])
        >>> taken = obs.take(3)
        >>> await taken.to_list()
        [1, 2, 3]
        """
        source = self

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            count = 0

            async def on_next(value: A) -> Acknowledgement:
                nonlocal count
                count += 1
                if count < n:
                    return await subscriber.on_next(value)
                else:
                    # call on_completed when maximum count is reached
                    # Call on_next one last time
                    await subscriber.on_next(value)
                    # Call on_completed
                    await subscriber.on_completed()
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    def take_while_exclusive(self: Observable[A], predicate: Callable[[A], bool]) -> 'Observable[A]':
        """Take values until predicate is false.

        Stops **before** emitting the first value where `predicate` is false.

        Parameters
        ----------
        predicate : Callable
            Function to test each value.

        Returns
        -------
        Observable
            Observable emitting values until predicate is false.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3, 4, 5])
        >>> taken = obs.take_while_exclusive(lambda x: x < 4)
        >>> await taken.to_list()
        [1, 2, 3]
        """
        source = self

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                if predicate(value):
                    return await subscriber.on_next(value)
                else:
                    # call on_completed when predicate violated
                    await subscriber.on_completed()
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    def take_while_inclusive(self: Observable[A], predicate: Callable[[A], bool]) -> 'Observable[A]':
        """Take values until predicate is false.

        Stops **after** emitting the last value where `predicate` is true.

        Parameters
        ----------
        predicate : Callable
            Function to test each value.

        Returns
        -------
        Observable
            Observable emitting values until predicate is false.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3, 4, 5])
        >>> taken = obs.take_while_inclusive(lambda x: x < 4)
        >>> await taken.to_list()
        [1, 2, 3, 4]
        """
        source = self

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
                if predicate(value):
                    return await subscriber.on_next(value)
                else:
                    # include the violating element in the stream
                    await subscriber.on_next(value)
                    # call on_completed when predicate violated
                    await subscriber.on_completed()
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(
                on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
            )

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    def take_last(self: Observable[A], n: int) -> 'Observable[A]':
        """Take the last n values from the Observable.

        Parameters
        ----------
        n : int
            Number of last values to take.

        Returns
        -------
        Observable
            Observable emitting the last n values.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3, 4, 5])
        >>> taken = obs.take_last(2)
        >>> await taken.to_list()
        [4, 5]
        """
        source = self
        buffer = deque(maxlen=n)

        async def subscribe_async(subscriber: Subscriber[A]) -> None:
            async def on_next(value: A) -> Acknowledgement:
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

    async def first(self) -> A_co:
        """Get the first emitted value from the Observable.

        Returns
        -------
        A_co
            The first value emitted, or raises if empty.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3])
        >>> await obs.first()
        1
        """
        items = await self.take(1).to_list()
        return items[0]

    async def run_to_completion(self) -> int:
        """Run the Observable and count emitted values.

        Runs the Observable until completion, counting the
        number of final values emitted.

        Returns
        -------
        int
            Number of values emitted before completion.

        Examples
        --------
        >>> obs = Observable.from_iterable([1, 2, 3]).take(2)
        >>> await obs.run_to_completion()
        2
        """
        count = 0

        async def on_next(value: Any) -> Acknowledgement:
            nonlocal count
            count = count + 1
            return Acknowledgement.ok

        subscriber = create_subscriber(on_next=on_next, on_completed=None, on_error=None)

        await self.subscribe(subscriber)

        return count

    async def run_until_timeout(self, seconds: float) -> int:
        """Run the Observable until a timeout occurs.

        Runs the Observable until `seconds` elapse, counting the
        number of values emitted in that time.

        Parameters
        ----------
        seconds : float
            Number of seconds to run for.

        Returns
        -------
        int
            Count of values emitted before timeout.

        Examples
        --------
        >>> obs = Observable.from_interval(0.1)
        >>> await obs.run_until_timeout(0.3)
        # Emits ~3 values in 0.3 seconds
        """
        count = 0

        class AnonymousSubscriber(Subscriber[A]):
            async def on_next(self, value: A) -> Acknowledgement:
                nonlocal count
                count = count + 1
                return Acknowledgement.ok

            async def on_error(self, error: Exception) -> None:
                task_group.cancel_scope.cancel()
                raise error

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
