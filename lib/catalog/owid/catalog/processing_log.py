import os
import random
import sys
import tempfile
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple, TypeVar

from dataclasses_json import dataclass_json

# Environment variable such that, if True, the processing log will be updated, if False, the log will always be empty.
# If not defined, assume False.
PROCESSING_LOG = bool(os.getenv("PROCESSING_LOG", False))


T = TypeVar("T")


def pruned_json(cls: T) -> T:
    orig = cls.to_dict  # type: ignore

    # only keep non-null public variables
    # make sure to call `to_dict` of nested objects as well
    cls.to_dict = lambda self, **kwargs: {  # type: ignore
        k: getattr(self, k).to_dict(**kwargs) if hasattr(getattr(self, k), "to_dict") else v
        for k, v in orig(self, **kwargs).items()
        if not k.startswith("_") and v not in [None, [], {}]
    }

    return cls


@pruned_json
@dataclass_json
@dataclass(frozen=True)
class LogEntry:
    variable: str
    parents: Tuple[str, ...]
    operation: str
    target: str
    comment: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        ...

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "LogEntry":
        ...

    def clone(self, **kwargs):
        """Clone the log entry, optionally overriding some attributes."""
        d = self.to_dict()
        d.update(**kwargs)
        return LogEntry.from_dict(d)


class ProcessingLog(List[LogEntry]):
    # hack for dataclasses_json
    __args__ = (LogEntry,)

    # NOTE: calling this method `as_dict` is intentional, otherwise it gets called
    # by dataclass_json
    def as_dict(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self]

    def add_entry(
        self,
        variable: str,
        parents: List[Any],
        operation: str,
        target: Optional[str] = None,
        comment: Optional[str] = None,
    ) -> None:
        if not PROCESSING_LOG:
            # Avoid any processing
            return

        # Renaming has no effect, skip it.
        # TODO: is this right?
        if len(self) > 0 and self[-1].variable == variable and operation == "rename":
            return

        # TODO: Parents currently can be anything. Here we should ensure that they are strings. For example, we could
        # extract the name of the parent if it is a variable.

        # List names of variables and scalars (or other objects passed in variables).
        new_parents = []
        for parent in parents:
            # Variable instance
            if hasattr(parent, "metadata"):
                parent = parent.metadata

            # VariableMeta instance
            if hasattr(parent, "processing_log"):
                if len(parent.processing_log) == 0:
                    new_parents.append(variable)
                else:
                    new_parents.append(parent.processing_log[-1].target)
            elif hasattr(parent, "name"):
                new_parents.append(parent.name)
            else:
                new_parents.append(str(parent))

        if not target:
            target = f"{variable}#{random_hash()}"

        # Define new log entry.
        entry = LogEntry(
            variable=variable, parents=tuple(new_parents), operation=operation, target=target, comment=comment
        )

        # TODO: can this duplication happen?
        if entry in self:
            raise NotImplementedError("Fixme")

        self.append(entry)

    def display(self, output: Literal["text", "html"] = "html", auto_open=True):
        """Displays processing log as a Mermaid diagram in a browser or as a text."""

        pl = preprocess_log(self)

        mermaid_diagram = "\n".join(_mermaid_diagram(pl))

        if output == "text":
            return mermaid_diagram

        s = (
            """
<html>
<head>
    <title>Mermaid Diagram</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/mermaid/10.4.0/mermaid.min.js"></script>
    <style>
        .mermaid {
          margin: auto;
        }
    </style>
</head>
<body>
    <div class="mermaid">
        """
            + mermaid_diagram
            + """
    </div>

    <script>
        mermaid.initialize({startOnLoad:true});
    </script>
</body>
</html>
"""
        )

        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as temp:
            temp.write(s.encode())
            temp.seek(0)

        if auto_open:
            webbrowser.open("file://" + os.path.realpath(temp.name))


@contextmanager
def disable_processing_log():
    module = sys.modules[__name__]

    original_value = getattr(module, "PROCESSING_LOG")
    module.PROCESSING_LOG = False  # type: ignore
    try:
        yield
    finally:
        module.PROCESSING_LOG = original_value  # type: ignore


def random_hash():
    return random.randint(0, int(1e10))


def _mermaid_diagram(pl: list[LogEntry]):
    yield "graph TB;"

    for r in pl:
        # TODO: multiple parents join with `&`
        for parent in r.parents:
            # constant or unknown column, add random hash to get unique vertices
            # if / is in parent, it means it's a path (TODO: make it more robust)
            if "#" not in parent and "/" not in parent:
                parent += f"#{random_hash()}"

            yield f"{parent}[{parent.split('#')[0]}] --> |{r.operation}| {r.target}[{r.target.split('#')[0]}]"


def preprocess_log(pl: ProcessingLog) -> ProcessingLog:
    # try to merge rename with previous operation
    new_pl = []
    last_r = None
    seen_r = set()
    for r in pl:
        # TODO: this should never happen
        if str(r) in seen_r:
            raise NotImplementedError("Fixme")

        if last_r and r.operation == "rename":
            # operation is just renaming, we can merge it with the previous one
            if last_r.target == r.parents[0]:
                new_pl[-1] = last_r.clone(target=r.target, variable=r.variable)
                continue

        last_r = r
        new_pl.append(r)
        seen_r.add(str(r))

    return ProcessingLog(new_pl)
