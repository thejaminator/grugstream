import datetime
from pathlib import Path

import pytest

from grubstream.core import (
    Observable,
)  # replace 'your_module' with the actual module name


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
async def test_from_file(tmp_path: Path):
    # Create a test file
    file_path = tmp_path / "testfile.txt"
    file_path.write_text("line1\nline2\nline3\n")

    # Create an observable from the file
    observable = Observable.from_file(file_path)

    # Run the observable and collect the output
    items = await observable.to_list()

    expected_output = ["line1\n", "line2\n", "line3\n"]
    assert items == expected_output

@pytest.mark.asyncio
async def test_to_file(tmp_path: Path):
    # Create some test data
    test_data = ["Hello", "world!", "This", "is", "a", "test."]
    observable = Observable.from_iterable(test_data)

    # Set up the output file path
    file_path = tmp_path / "testfile.txt"

    # Write to file
    await observable.to_file(file_path)

    # Check the file contents
    file_contents = file_path.read_text().splitlines()
    assert file_contents == test_data