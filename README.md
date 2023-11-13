# Grugstream

[![Build Status](https://github.com/thejaminator/grugstream/actions/workflows/main.yml/badge.svg)](https://github.com/thejaminator/grugstream/actions/workflows/main.yml)
[![python](https://img.shields.io/pypi/pyversions/grugstream.svg)](https://pypi.org/project/grugstream)

```
pip install grugstream
```

See the [documentation](https://thejaminator.github.io/grugstream/)
## Introduction

The grugstream library provides an easy way to work with asynchronous/ reactive / streaming programming in Python.

Set up data processing pipelines that are faster, use less memory and are easy to understand. 

Works with anyio - which means it works with asyncio and trio.

This library is inspired by
- Rxpy, aioreactive
- monix

Philosophy - when you hit dot on your keyboard - you should have everything you need.

Also - everything is statically typed!

## Getting Started

### Basic Example
What we always do is
1. Create an observable (A stream that is not running yet)
2. Transform it things like `map` or `filter`
3. Run it. 
   - For example, `to_list` will run the observable and collect the results into a list.
   - `run_to_completion` will run the observable until it completes
   - `to_file_appending` will run the observable and write the results to a file
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

    # Actually start the stream and collect the results into a list
    results = await observable.to_list()

    for response in results:
        print(response)


anyio.run(main)
```

### Map operators - Parallel Example

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
Suppose you have multiple api calls to run.  
You  want all the api calls to be run in parallel with each other - 
the items doing the 2nd api call don't need to wait for all the items for the 1st api call to complete.
And maybe you want to stream to a file while it completes.
Thats when streaming really shines.
```python
import random
from pathlib import Path
from typing import List, Optional

import anyio

from grugstream import Observable


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
        .map_async_par(lambda item: mock_http_call_to_google(item))
        .map_async_par(lambda item: mock_api_call_that_returns_list(item))
        .flatten_iterable()  # Flatten the list into individual items
        .map_async_par(lambda item: mock_api_call_that_returns_optional(item))
        .print()
        .flatten_optional()  # Remove None values
    )

    # Write the results to a file
    await observable.take(100).to_file(Path("results.txt"))


anyio.run(main)
```

## Logging and debugging, print and tqdm
We provide a `print` and `tqdm` operator to help you debug your streams.

```python
import anyio
from tqdm import tqdm

from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(0.1)
    return f"Google Response for {item}"


async def main():
    observable = (
        Observable.from_repeat("query", 0.1)
        .throttle(1)  # don't spam google too much!
        .map_async_par(lambda item: mock_http_call_to_google(item))
        # Show a progress bar that should show ~1 it/s
        .tqdm(tqdm_bar=tqdm(desc="Google observable"))
        # Print the elements
        .print()
    )

    await observable.take(1000).run_to_completion()


anyio.run(main)
```


## for_each operator - side effects
Sometimes you want to do something with the elements of the stream, but you don't want to change the stream itself.
For example, you might want to write some intermediate items to a file.

```python
import anyio
from pathlib import Path
from grugstream import Observable


# Mock async function simulating an HTTP call to Google
async def mock_http_call_to_google(item: str) -> str:
    await anyio.sleep(0.1)
    return f"Google Response for {item}"


async def main():
    my_list = []
    observable = (
        Observable.from_repeat("query", 0.1)
        .map_async_par(lambda item: mock_http_call_to_google(item))
        # What's google's response? Let's write it to a file
        .for_each_to_file(
            file_path=Path("results.txt"),
        )
        # Let's also append it to a list to print
        .for_each(lambda item: my_list.append(item))
        .map(lambda item: item.upper())
        .print()
    )

    await observable.take(1000).run_to_completion()
    print(my_list)


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

## Back-pressure, Buffearing, Throttling

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