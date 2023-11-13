import datetime

import anyio

from grugstream import Observable


async def main():
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
    assert time_delta < datetime.timedelta(seconds=1)


if __name__ == "__main__":
    anyio.run(main)
