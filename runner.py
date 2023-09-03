import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Awaitable, TypeVar, Generic


class Acknowledgement(str, Enum):
    ok = "ok"
    stop = "stop"


A_co = TypeVar("A_co", covariant=True)
B = TypeVar("B")
B_co = TypeVar("B_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


def create_observable(subscribe: Callable[["Subscriber[A_co]"], Awaitable[None]]) -> "Observable[A_co]":
    class AnonObservable(Observable[A_co]):  # type: ignore
        async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
            await subscribe(subscriber)

    return AnonObservable()


class Observable(ABC, Generic[A_co]):
    """Abstract base class for Observable."""

    @staticmethod
    def from_iterable(iterable: list[A_co]) -> "Observable[A_co]":
        class IterableObservable(Observable[A_co]):  # type: ignore
            async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
                for item in iterable:
                    await subscriber.on_next(item)
                await subscriber.on_completed()

        return IterableObservable()

    @staticmethod
    def interval(time_unit: float) -> 'Observable[int]':
        async def emit_values(subscriber: Subscriber[int], counter: int = 0) -> None:
            ack = Acknowledgement.ok
            while ack == Acknowledgement.ok:
                ack = await subscriber.on_next(counter)
                counter += 1
                await asyncio.sleep(time_unit)

        async def subscribe_async(subscriber: Subscriber[int]) -> None:
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

    def print(
        self: "Observable[A_co]", printer: Callable[[A_co], None] = print, prefix: str = ""
    ) -> "Observable[A_co]":
        return self.for_each(lambda x: printer(f"{prefix}{x}"))  # type: ignore

    async def to_list(self) -> list[A_co]:
        result = []

        async def on_next(value: A_co) -> Acknowledgement:
            result.append(value)
            return Acknowledgement.ok

        list_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(list_subscriber)

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
                    print("Stopping")
                    return Acknowledgement.stop

            take_subscriber = create_subscriber(on_next=on_next)

            await source.subscribe(take_subscriber)

        return create_observable(subscribe_async)

    async def run_to_completion(self) -> None:
        await self.subscribe(RunToCompletionSubscriber)


class Subscriber(Generic[T_contra]):
    """Base class for Subscriber."""

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


async def main():
    my_subscriber = PrintSubscriber()

    my_observable: Observable[int] = Observable.interval(time_unit=0.1)
    stream = my_observable.map(lambda x: x * 2).print().filter(lambda x: x % 10 == 0).take(15)
    await stream.subscribe(my_subscriber)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
