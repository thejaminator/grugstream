# import annotations for 3.9 compat
from __future__ import annotations
from abc import abstractmethod
from typing import TypeVar, Callable, Awaitable, Generic

from grugstream.acknowledgement import Acknowledgement

T_contra = TypeVar("T_contra", contravariant=True)


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


class PrintSubscriber(Subscriber[T_contra]):
    async def on_next(self, value: T_contra) -> Acknowledgement:
        print(value)
        return Acknowledgement.ok

    async def on_error(self, error: Exception) -> None:
        print(f"Error occurred: {error}")

    async def on_completed(self) -> None:
        print("Completed")


def create_subscriber(
    on_next: Callable[[T_contra], Awaitable[Acknowledgement]] | None,
    on_error: Callable[[Exception], Awaitable[None]] | None,
    on_completed: Callable[[], Awaitable[None]] | None,
) -> Subscriber[T_contra]:
    """Create an Subscriber from functions.
    This is so that you don't need to create a class everything
    """

    class AnonymousSubscriber(Subscriber[T_contra]):  # type: ignore
        async def on_next(self, value: T_contra) -> Acknowledgement:
            if on_next is not None:
                return await on_next(value)
            return Acknowledgement.ok

        async def on_error(self, error: Exception) -> None:
            if on_error is not None:
                await on_error(error)
            raise error

        async def on_completed(self) -> None:
            if on_completed is not None:
                await on_completed()

    return AnonymousSubscriber()
