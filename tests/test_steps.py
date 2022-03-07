#
#  test_steps.py
#

"""
Test that the different step types work as expected.
"""

from contextlib import contextmanager
from typing import Iterator
import random
import string
import shutil
from unittest.mock import patch

from owid.catalog import Dataset

from etl import paths
from etl.steps import (
    DataStep,
    DataStepPrivate,
    compile_steps,
    to_dependency_order,
    Step,
)


def _create_mock_py_file(step_name: str) -> None:
    py_file = paths.STEP_DIR / "data" / f"{step_name}.py"
    assert not py_file.exists()
    with open(str(py_file), "w") as ostream:
        print(
            """
from owid.catalog import Dataset
def run(dest_dir):
    Dataset.create_empty(dest_dir)
            """,
            file=ostream,
        )


def test_data_step():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStep(step_name, []).run()
        Dataset((paths.DATA_DIR / step_name).as_posix())


@contextmanager
def temporary_step() -> Iterator[str]:
    "Make a step in the etl/ directory, but clean up afterwards."
    name = "".join(random.choice(string.ascii_lowercase) for i in range(10))
    try:
        yield name
    finally:
        data_dir = paths.DATA_DIR / name

        if data_dir.is_dir():
            shutil.rmtree(data_dir.as_posix())

        py_file = paths.STEP_DIR / "data" / f"{name}.py"
        ipy_file = paths.STEP_DIR / "data" / f"{name}.ipynb"

        if py_file.exists():
            py_file.unlink()

        if ipy_file.exists():
            ipy_file.unlink()


def test_topological_sort():
    "Check that a dependency will be scheduled to run before things that need it."
    dag = {"a": ["b", "c"], "b": ["c"]}
    assert to_dependency_order(dag, [], []) == ["c", "b", "a"]


@patch("etl.steps.parse_step")
def test_selection_selects_children(parse_step):
    "When you pick a step, it should rebuild everything that depends on that step."
    parse_step.side_effect = lambda name, _: DummyStep(name)

    dag = {"a": ["b", "c"], "d": ["a"]}

    # selecting "c" should cause "c" -> "a" -> "d" to all be selected
    #                            "b" to be ignored
    steps = compile_steps(dag, ["c"], [])
    assert len(steps) == 3
    assert set(s.path for s in steps) == {"c", "a", "d"}


@patch("etl.steps.parse_step")
def test_selection_selects_parents(parse_step):
    "When you pick a step, it should select everything that step depends on."
    parse_step.side_effect = lambda name, _: DummyStep(name)

    dag = {"a": ["b"], "d": ["a"], "c": ["a"]}

    # selecting "d" should cause "b" -> "a" -> "d" to all be selected
    #                            "c" to be ignored
    steps = compile_steps(dag, ["d"], [])
    assert len(steps) == 3
    assert set(s.path for s in steps) == {"b", "a", "d"}


class DummyStep(Step):
    def __init__(self, name: str):
        self.path = name

    def __repr__(self):
        return self.path


def test_date_step_private():
    with temporary_step() as step_name:
        _create_mock_py_file(step_name)
        DataStepPrivate(step_name, []).run()
        ds = Dataset((paths.DATA_DIR / step_name).as_posix())
        assert not ds.metadata.is_public
