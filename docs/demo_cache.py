import random
from pathlib import Path
from typing import TypeVar, Generic, Type, Callable, Hashable

import anyio
from anyio import open_file
from pydantic import BaseModel
from slist import Slist
from tqdm import tqdm

from grugstream.core import Observable


async def mock_openai_call(prompt: str) -> str:
    # randdom wait between 0.1 and 10 seconds
    rand_wait = random.uniform(0.1, 2)
    await anyio.sleep(rand_wait)
    return f"Yoo I'm gpt-4 responding about your {prompt}"


class TaskSpec(BaseModel):
    prompt: str
    task_hash: str


class TaskOutput(BaseModel):
    task_spec: TaskSpec
    inference_output: str


class Stage2aTaskOutput(BaseModel):
    stage_one_output: TaskOutput
    stage_2_a_output: TaskOutput


class Stage2bTaskOutput(BaseModel):
    stage_one_output: TaskOutput
    stage_2_a_output: TaskOutput
    stage_2_b_output: TaskOutput


class Stage2cTaskOutput(BaseModel):
    stage_one_output: TaskOutput
    stage_2_a_output: TaskOutput
    stage_2_b_output: TaskOutput
    stage_2_c_output: TaskOutput


async def handle_task_spec(task_spec: TaskSpec) -> TaskOutput:
    return TaskOutput(task_spec=task_spec, inference_output=await mock_openai_call(task_spec.prompt))


async def handle_stage_two_a(stage_one_output: TaskOutput) -> Stage2aTaskOutput:
    inference_output = await mock_openai_call("Stage two a:" + stage_one_output.inference_output)
    task_output = TaskOutput(task_spec=stage_one_output.task_spec, inference_output=inference_output)
    return Stage2aTaskOutput(
        stage_one_output=stage_one_output,
        stage_2_a_output=task_output,
    )


async def handle_stage_two_b(task_spec: Stage2aTaskOutput) -> Stage2bTaskOutput:
    inference_output = await mock_openai_call("Stage two b:" + task_spec.stage_2_a_output.inference_output)
    task_output = TaskOutput(task_spec=task_spec.stage_2_a_output.task_spec, inference_output=inference_output)
    return Stage2bTaskOutput(
        stage_one_output=task_spec.stage_one_output,
        stage_2_a_output=task_spec.stage_2_a_output,
        stage_2_b_output=task_output,
    )


async def handle_stage_two_c(task_spec: Stage2bTaskOutput) -> Stage2cTaskOutput:
    inference_output = await mock_openai_call("Stage two c:" + task_spec.stage_2_b_output.inference_output)
    task_output = TaskOutput(task_spec=task_spec.stage_2_b_output.task_spec, inference_output=inference_output)
    return Stage2cTaskOutput(
        stage_one_output=task_spec.stage_one_output,
        stage_2_a_output=task_spec.stage_2_a_output,
        stage_2_b_output=task_spec.stage_2_b_output,
        stage_2_c_output=task_output,
    )


def load_task_specs() -> list[TaskSpec]:
    return [TaskSpec(prompt=f"I'm a prompt {i}", task_hash=str(i)) for i in range(10)]


GenericBaseModel = TypeVar('GenericBaseModel', bound=BaseModel)


class GenericCache(Generic[GenericBaseModel]):
    def __init__(
        self,
        item_type: Type[GenericBaseModel],
        # in reality this could also just be a protocol to read the correct file_path to write to
        file_path_func: Callable[[GenericBaseModel], Path],
        # hash func should be unique for each file path
        hash_func: Callable[[GenericBaseModel], str],
        # items loaded in the cache
        items: dict[Path, dict[str, GenericBaseModel]],
    ):
        self.item_type = item_type
        self.file_path_func = file_path_func
        self.hash_func = hash_func
        self.items = items

    def item_in_cache(self, item: GenericBaseModel) -> bool:
        _hash = self.hash_func(item)
        path = self.file_path_func(item)
        if path in self.items:
            return _hash in self.items[path]
        return False

    async def async_write_to_path(self) -> None:
        # TODO
        ...

    async def update_cache(self) -> None:
        # TODO
        ...



class StageOneCache:
    def __init__(self, items: dict[str, TaskOutput], save_path: Path):
        # in reality its more of dict[path, dict[hash, TaskOutput]]
        self.dict = items
        self.__update_counter = 0
        self.save_path = save_path

    def item_in_cache(self, task_spec: TaskSpec) -> bool:
        return task_spec.task_hash in self.dict

    def add_to_cache(self, task_output: TaskOutput, dump_every: int) -> None:
        self.dict[task_output.task_spec.task_hash] = task_output
        self.__update_counter += 1
        if self.__update_counter % dump_every == 0:
            self.write_to_path()

    async def add_to_cache_async(self, task_output: TaskOutput, dump_every: int) -> None:
        self.dict[task_output.task_spec.task_hash] = task_output
        self.__update_counter += 1
        if self.__update_counter % dump_every == 0:
            await self.async_write_to_path()

    def write_to_path(self) -> None:
        with open(self.save_path, "w") as f:
            for task_hash, task_output in self.dict.items():
                f.write(task_output.model_dump_json())
                f.write("\n")

    async def async_write_to_path(self) -> None:
        # the dict may be updated while we are writing
        to_write = self.dict.copy()
        async with await open_file(self.save_path, "w") as f:
            for task_hash, task_output in to_write.items():
                await f.write(task_output.model_dump_json())
                await f.write("\n")

    @staticmethod
    def from_path(path: Path) -> "StageOneCache":
        # create if not exists
        path.touch(exist_ok=True)
        items = Slist()
        with open(path, "r") as f:
            for line in f.readlines():
                items.append(TaskOutput.model_validate_json(line))
        new = StageOneCache(items={}, save_path=path)
        for item in items:
            new.dict[item.task_spec.task_hash] = item
        return new


async def main():
    task_specs: list[TaskSpec] = load_task_specs()
    stage_one_path = Path("../stage_one.jsonl")
    stage_one_cache = StageOneCache.from_path(stage_one_path)
    await (
        Observable.from_iterable(task_specs)
        # .filter(lambda x: not stage_one_cache.item_in_cache(x))
        # you can pass the caches into handle_task_spec to make it read nicer
        .map_async_par(handle_task_spec, max_par=20)
        .for_each_async(lambda x: stage_one_cache.add_to_cache_async(x, dump_every=10))
        .print()
        .map_async_par(handle_stage_two_a, max_par=10)
        .print()
        .tqdm(tqdm_bar=tqdm(desc="Stage two a"))
        .map_async_par(handle_stage_two_b, max_par=10)
        .print()
        .to_list()
    )
    print("done")

    await stage_one_cache.async_write_to_path()


if __name__ == "__main__":
    anyio.run(main)
