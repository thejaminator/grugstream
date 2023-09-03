from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Awaitable, TypeVar, Generic
import asyncio


class Acknowledgement(str, Enum):
    ok = "ok"
    stop = "stop"


A_co = TypeVar("A_co", covariant=True)
B = TypeVar("B")
B_co = TypeVar("B_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class Observable(ABC, Generic[A_co]):
    """Abstract base class for Observable."""

    @abstractmethod
    async def subscribe(self, subscriber: "Subscriber[A_co]") -> None:
        """Subscribe async subscriber."""
        pass

    def map(self, func: Callable[[A_co], B_co]) -> "Observable[B_co]":
        return MappedObservable(source=self, func=func)

    def filter(self, predicate: Callable[[A_co], bool]) -> "Observable[A_co]":
        return FilteredObservable(source=self, predicate=predicate)

    @staticmethod
    def from_iterable(iterable: list[A_co]) -> "Observable[A_co]":
        class IterableObservable(Observable[A_co]):  # type: ignore
            async def subscribe(self, subscriber: Subscriber[A_co]) -> None:
                for item in iterable:
                    await subscriber.on_next(item)
                await subscriber.on_completed()

        return IterableObservable()

    async def to_list(self) -> list[A_co]:
        result = []

        async def on_next(value: A_co) -> Acknowledgement:
            result.append(value)
            return Acknowledgement.ok

        list_subscriber = create_subscriber(on_next=on_next)

        await self.subscribe(list_subscriber)

        return result


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
            transformed_value = self.func(value)
            await subscriber.on_next(transformed_value)
            return Acknowledgement.ok

        map_subscriber: Subscriber[A_co] = create_subscriber(  # type: ignore
            on_next=on_next, on_error=subscriber.on_error, on_completed=subscriber.on_completed
        )

        await self.source.subscribe(map_subscriber)

async def main():
    my_subscriber = PrintSubscriber()

    my_observable: Observable[int] = Observable.from_iterable([1, 2, 3])
    stream = my_observable.map(lambda x: x * 2)
    to_list = await stream.to_list()
    await stream.subscribe(my_subscriber)
    await stream.subscribe(my_subscriber)

    assert to_list == [2, 4, 6]

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
