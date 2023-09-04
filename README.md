# Grugstream

## Introduction

The grugstream library provides an easy way to work with asynchronous/ reactive / streaming programming in Python. It allows you to model and process asynchronous streams of events using non-blocking back-pressure.
Works with anyio - which means it works with asyncio and trio.

This library is inspired by
- Rxpy, aioreactive
- monix

Philosophy - when you hit dot on your keyboard - you should have everything you need.

Also - everything is statically typed!

## Getting Started

### Basic Example

```python
import anyio
from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    # Simulate the asynchronous delay of an HTTP request
    await anyio.sleep(1)
    return f"Response from Google {item}"


async def main():
    # Create an observable, and call google for each item
    observable = (
        Observable.from_iterable(["one", "two", "three"])
        # this is the same as map, but it's for async functions
        .map_async(lambda item: mock_http_call_to_google(item))
    )

    # Actually start the stream - results into a list
    results = await observable.to_list()

    for response in results:
        print(response)


anyio.run(main)
```

### Parallel Example

Running things in parallel is as simple as calling `map_async_par` instead of `map_async`:

```python
import anyio
from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    # Simulate the asynchronous delay of an HTTP request
    await anyio.sleep(1)
    return f"Response from Google {item}"


async def main():
    # Create an observable, and call google for each item
    observable = (
         # repeat every 0.1 seconds
        Observable.from_repeat("one", 0.1)
        # at any given time, there will be at most 50 concurrent calls to google
        .map_async_par(lambda item: mock_http_call_to_google(item), max_par=50)
    )

    # Actually start the stream - results into a list
    # Let's take only 20 results
    results = await observable.take(20).to_list()

    for response in results:
        print(response)


anyio.run(main)
```

## Chaining other api calls

```python
import random

import anyio
from grugstream import Observable
from typing import List, Optional
from pathlib import Path


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(1)
    return f"Google Response for {item}"


# Mock async function simulating an API call that returns a list of items
async def mock_api_call_that_returns_list(item: str) -> List[str]:
    await anyio.sleep(0.5)
    return [f"Item {i} from {item}" for i in range(3)]


# Mock async function simulating an API call that returns an Optional value
async def mock_api_call_that_returns_optional(item: str) -> Optional[str]:
    await anyio.sleep(0.2)
    maybe_yes = random.choice([True, False])
    return item if maybe_yes else None


async def main():
    observable = (
        Observable.from_repeat("query", 0.1)
        .throttle(0.5)  # don't spam google too much!
        .map_async(lambda item: mock_http_call_to_google(item))
        .map_async(lambda item: mock_api_call_that_returns_list(item))
        .flatten_iterable()  # Flatten the list into individual items
        .map_async(lambda item: mock_api_call_that_returns_optional(item))
        .print()
        .flatten_optional()  # Remove None values
    )

    # Write the results to a file
    await observable.take(20).to_file(Path("results.txt"))


anyio.run(main)
```


## Building an Observable

This library provides several utility methods for creating observables:

### From Existing Data

- `from_iterable(iterable)`: Create an observable from a Python iterable like a list or a tuple.
- `from_async_iterable(iterable)`: Create an observable from an asynchronous iterable.
- `from_one(value)`: Create an observable that emits a single value.
- `from_one_option(value)`: Create an observable that emits a single value or nothing if the value is `None`.

Example:

```python
from grugstream import Observable
observable = Observable.from_iterable([1, 2, 3])
```


## Transforming Observables

### `map`

Applies a function to all elements in the source observable.

```python
observable = Observable.from_iterable([1, 2, 3])
new_observable = observable.map(lambda x: x * 2)
```

### `filter`

Filters out elements that do not match a given predicate.

```python
observable = Observable.from_iterable([1, 2, 3])
filtered_observable = observable.filter(lambda x: x > 1)
```

### `flatten_iterable`

Transforms an observable of iterables into an observable of the individual items.

```python
observable = Observable.from_iterable([[1, 2], [3, 4]])
flattened_observable = observable.flatten_iterable()
```

## Back-pressure, Buffering, Throttling

The library supports back-pressure to ensure that the producer and consumer are in sync. There are also methods like `throttle(seconds)` to control the rate of emissions.

```python
throttled_observable = observable.throttle(1.0)  # Emits at most one item per second
```

## Subscription and Error Handling

You can use the `Subscriber` class to define custom subscribers. It has three methods:

- `on_next(value)`: Called when a new value is emitted.
- `on_error(error)`: Called when an error occurs.
- `on_completed()`: Called when the observable completes.

In general, I hope that you wouldn't have to implement your own subscriber.
Most things you want to do can be done by chaining operators such as `map` and `filter`.