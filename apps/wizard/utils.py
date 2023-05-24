"""General utils.

TODO: Should probably re-order this file and split it into multiple files.
"""
import argparse
import datetime as dt
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Type, cast

import jsonref
import jsonschema
import ruamel.yaml
import streamlit as st
import yaml
from cookiecutter.main import cookiecutter
from MySQLdb import OperationalError
from owid import walden
from owid.catalog.utils import validate_underscore
from pydantic import BaseModel
from typing_extensions import Self

from etl import config
from etl.db import get_connection
from etl.files import apply_black_formatter_to_files
from etl.paths import (
    APPS_DIR,
    DAG_DIR,
    LATEST_POPULATION_VERSION,
    LATEST_REGIONS_VERSION,
    STEP_DIR,
)
from etl.steps import DAG

DAG_WALKTHROUGH_PATH = DAG_DIR / "walkthrough.yml"
WALDEN_INGEST_DIR = Path(walden.__file__).parent.parent.parent / "ingests"

# Load latest dataset versions
DATASET_POPULATION_URI = f"data://garden/demography/{LATEST_POPULATION_VERSION}/population"
DATASET_REGIONS_URI = f"data://garden/regions/{LATEST_REGIONS_VERSION}/regions"

# use origins in walkthrough
# WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "1") == "1"
WALKTHROUGH_ORIGINS = os.environ.get("WALKTHROUGH_ORIGINS", "0") == "1"

# DAG dropdown options
dag_files = sorted(os.listdir(DAG_DIR))
dag_not_add_option = "(do not add to DAG)"
ADD_DAG_OPTIONS = [dag_not_add_option] + dag_files

# Date today
DATE_TODAY = dt.date.today().strftime("%Y-%m-%d")

# Get current directory
CURRENT_DIR = Path(__file__).parent

# Phases accepted
PHASES = Literal["all", "snapshot", "meadow", "garden", "grapher", "charts"]

# Paths to cookiecutter files
COOKIE_SNAPSHOT = APPS_DIR / "wizard" / "templating" / "cookiecutter" / "snapshot"
COOKIE_MEADOW = APPS_DIR / "wizard" / "templating" / "cookiecutter" / "meadow"
COOKIE_GARDEN = APPS_DIR / "wizard" / "templating" / "cookiecutter" / "garden"
COOKIE_GRAPHER = APPS_DIR / "wizard" / "templating" / "cookiecutter" / "grapher"
# Paths to markdown templates
MD_SNAPSHOT = APPS_DIR / "wizard" / "templating" / "markdown" / "snapshot.md"
MD_MEADOW = APPS_DIR / "wizard" / "templating" / "markdown" / "meadow.md"
MD_GARDEN = APPS_DIR / "wizard" / "templating" / "markdown" / "garden.md"
MD_GRAPHER = APPS_DIR / "wizard" / "templating" / "markdown" / "grapher.md"


if WALKTHROUGH_ORIGINS:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "dataset_title_owid": "Dummy OWID dataset title",
        "dataset_description_owid": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "date_published": "2020-01-01",
        "producer": "Dummy producer",
        "citation_producer": "Dummy producer citation",
        "dataset_url_download": "https://raw.githubusercontent.com/owid/etl/master/apps/wizard/dummy_data.csv",
        "dataset_url_main": "https://www.url-dummy.com/",
        "license_name": "MIT dummy license",
    }
else:
    DUMMY_DATA = {
        "namespace": "dummy",
        "short_name": "dummy",
        "version": "2020-01-01",
        "walden_version": "2020-01-01",
        "snapshot_version": "2020-01-01",
        "name": "Dummy dataset",
        "description": "This\nis\na\ndummy\ndataset",
        "file_extension": "csv",
        "source_data_url": "https://raw.githubusercontent.com/owid/etl/master/apps/wizard/dummy_data.csv",
        "publication_date": "2020-01-01",
        "source_name": "Dummy short source citation",
        "source_published_by": "Dummy full source citation",
        "url": "https://www.url-dummy.com/",
    }

# state shared between steps
APP_STATE = {}


def validate_short_name(short_name: str) -> Optional[str]:
    """Validate short name."""
    try:
        validate_underscore(short_name, "Short name")
        return None
    except Exception as e:
        return str(e)


def add_to_dag(dag: DAG, dag_path: Path = DAG_WALKTHROUGH_PATH) -> str:
    """Add dag to dag_path file."""
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].update(dag)

    with open(dag_path, "w") as f:
        yml = ruamel.yaml.YAML()
        yml.indent(mapping=2, sequence=4, offset=2)
        yml.dump(doc, f)

    return yaml.dump({"steps": dag})


def remove_from_dag(step: str, dag_path: Path = DAG_WALKTHROUGH_PATH) -> None:
    with open(dag_path, "r") as f:
        doc = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)

    doc["steps"].pop(step, None)

    with open(dag_path, "w") as f:
        ruamel.yaml.dump(doc, f, Dumper=ruamel.yaml.RoundTripDumper)


def generate_step(cookiecutter_path: Path, data: Dict[str, Any], target_dir: Path) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        # create config file with data for cookiecutter
        config_path = cookiecutter_path / "cookiecutter.json"
        with open(config_path, "w") as f:
            json.dump(data, f, default=str)

        try:
            cookiecutter(
                cookiecutter_path.as_posix(),
                no_input=True,
                output_dir=temp_dir,
                overwrite_if_exists=True,
            )
        finally:
            config_path.unlink()

        # Apply black formatter to generated files.
        apply_black_formatter_to_files(file_paths=list(Path(temp_dir).glob("**/*.py")))

        shutil.copytree(
            Path(temp_dir),
            target_dir,
            dirs_exist_ok=True,
        )


def generate_step_to_channel(cookiecutter_path: Path, data: Dict[str, Any]) -> Path:
    assert {"channel", "namespace", "version"} <= data.keys()

    target_dir = STEP_DIR / "data" / data["channel"]
    generate_step(cookiecutter_path, data, target_dir)
    return target_dir / data["namespace"] / data["version"]


class classproperty(property):
    """Decorator."""

    def __get__(self, owner_self: Self, owner_cls: Self):
        return self.fget(owner_cls)  # type: ignore


class AppState:
    """Management of state variables shared across different apps."""

    steps: List[str] = ["snapshot", "meadow", "garden", "grapher", "explorers"]

    def __init__(self: "AppState") -> None:
        """Construct variable."""
        self.step = st.session_state["step_name"]
        self._init_steps()

    def _init_steps(self: "AppState") -> None:
        # Initiate dictionary
        if "steps" not in st.session_state:
            st.session_state["steps"] = {}
        for step in self.steps:
            if step not in st.session_state["steps"]:
                st.session_state["steps"][step] = {}

        # Add defaults
        st.session_state["steps"]["snapshot"]["snapshot_version"] = DATE_TODAY
        st.session_state["steps"]["snapshot"]["origin.date_accessed"] = DATE_TODAY
        st.session_state["steps"]["meadow"]["version"] = DATE_TODAY
        st.session_state["steps"]["meadow"]["snapshot_version"] = DATE_TODAY
        st.session_state["steps"]["garden"]["version"] = DATE_TODAY
        st.session_state["steps"]["garden"]["meadow_version"] = DATE_TODAY
        st.session_state["steps"]["grapher"]["version"] = DATE_TODAY
        st.session_state["steps"]["grapher"]["garden_version"] = DATE_TODAY

    def _check_step(self: "AppState") -> None:
        """Check that the value for step is valid."""
        if self.step is None or self.step not in self.steps:
            raise ValueError(f"Step {self.step} not in {self.steps}.")

    def get_variables_of_step(self: "AppState") -> Dict[str, Any]:
        """Get variables of a specific step.

        Variables are assumed to have keys `step.NAME`, based on the keys given in the widgets within a form.
        """
        return {
            cast(str, k): v for k, v in st.session_state.items() if isinstance(k, str) and k.startswith(f"{self.step}.")
        }

    def update(self: "AppState") -> None:
        """Update global variables of step.

        This is expected to be called when submitting the step's form.
        """
        self._check_step()
        print(f"Updating {self.step}...")
        st.session_state["steps"][self.step] = self.get_variables_of_step()

    def update_from_form(self, form: "StepForm") -> None:
        self._check_step()
        st.session_state["steps"][self.step] = form.dict()

    @property
    def state_step(self: "AppState") -> Dict[str, Any]:
        """Get state variables of step."""
        self._check_step()
        return st.session_state["steps"][self.step]

    def default_value(
        self: "AppState", key: str, previous_step: Optional[str] = None, default_last: Optional[Any] = ""
    ) -> str:
        """Get the default value of a variable.

        This is useful when setting good defaults in widgets (e.g. text_input).

        Priority of default value is:
            - Check if there is a value stored for this field in the current step.
            - If not, check if there is a value stored for this field in the previous step.
            - If not, use value given by `default_last`.
        """
        self._check_step()
        # Get name of previous step
        if previous_step is None:
            previous_step = self.previous_step
        # (1) Get value stored for this field (in current step)
        value_step = self.state_step.get(key)
        if value_step:
            return value_step
        # (2) If none, check if previous step has a value and use that one, otherwise (3) use empty string.
        key = key.replace(f"{self.step}.", f"{self.previous_step}.")
        return st.session_state["steps"][self.previous_step].get(key, default_last)

    def display_error(self: "AppState", key: str) -> None:
        """Get error message for a given key."""
        if "errors" in self.state_step:
            print(key)
            if msg := self.state_step.get("errors", {}).get(key, ""):
                st.error(msg)

    @property
    def previous_step(self: "AppState") -> str:
        """Get the name of the previous step.

        E.g. 'snapshot' is the step prior to 'meadow', etc.
        """
        self._check_step()
        idx = max(self.steps.index(self.step) - 1, 0)
        return self.steps[idx]

    def st_widget(
        self: "AppState",
        st_widget: Callable,
        default_last: Optional[str | bool | int] = "",
        **kwargs: str | int | List[str],
    ) -> None:
        """Wrap a streamlit widget with a default value."""
        key = cast(str, kwargs["key"])
        # Get default value (either from previous edits, or from previous steps)
        default_value = self.default_value(key, default_last=default_last)
        # Change key name, to be stored it in general st.session_state
        kwargs["key"] = f"{self.step}.{key}"
        # Default value for selectbox (and other widgets with selectbox-like behavior)
        if "options" in kwargs:
            options = cast(List[str], kwargs["options"])
            index = options.index(default_value) if default_value in options else 0
            kwargs["index"] = index
        # Default value for other widgets (if none is given)
        elif "value" not in kwargs:
            kwargs["value"] = default_value

        # Create widget
        widget = st_widget(**kwargs)
        # Show error message
        self.display_error(key)
        return widget

    @classproperty
    def args(cls: "AppState") -> argparse.Namespace:
        """Get arguments passed from command line."""
        if "args" in st.session_state:
            return st.session_state["args"]
        else:
            parser = argparse.ArgumentParser(description="This app lists animals")
            parser.add_argument("--phase")
            parser.add_argument("--run-checks", action="store_true")
            parser.add_argument("--dummy-data", action="store_true")
            args = parser.parse_args()
            st.session_state["args"] = args
        return st.session_state["args"]


class StepForm(BaseModel):
    """Form abstract class."""

    errors: Dict[str, Any] = {}

    def __init__(self: Self, **kwargs: str | int) -> None:
        """Construct parent class."""
        super().__init__(**kwargs)
        self.validate()

    @classmethod
    def filter_relevant_fields(cls: Type[Self], step_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter relevant fields from form."""
        return {k.replace(f"{step_name}.", ""): v for k, v in data.items() if k.startswith(f"{step_name}.")}

    @classmethod
    def from_state(cls: Type[Self]) -> Self:
        """Build object from session_state variables."""
        session_state = cast(Dict[str, Any], dict(st.session_state))
        data = cls.filter_relevant_fields(step_name=st.session_state["step_name"], data=session_state)
        return cls(**data)

    def validate(self: Self) -> None:
        """Validate form fields."""
        raise NotImplementedError("Needs to be implemented in the child class!")

    @property
    def metadata(self: Self) -> None:
        """Get metadata as dictionary."""
        raise NotADirectoryError("Needs to be implemented in the child class!")

    def to_yaml(self: Self, path: Path) -> None:
        """Export form (metadata) to yaml file."""
        with open(path, "w") as f:
            ruamel.yaml.dump(self.metadata, f, Dumper=ruamel.yaml.RoundTripDumper)

    def validate_schema(self: Self, schema: Dict[str, Any], ignore_keywords: Optional[List[str]] = None) -> None:
        """Validate form fields against schema.

        Note that not all form fields are present in the schema (some do not belong to metadata, but are needed to generate the e.g. dataset URI)
        """
        if ignore_keywords == []:
            ignore_keywords = []
        # Validator
        validator = jsonschema.Draft7Validator(schema)
        # Plain JSON
        schema_full = jsonref.replace_refs(schema)
        # Process each error
        errors = sorted(validator.iter_errors(self.metadata), key=str)  # get all validation errors
        for error in errors:
            # Get error type
            error_type = error.validator
            if error_type not in {"required", "type", "pattern"}:
                raise Exception(f"Unknown error type {error_type} with message {error.message}")
            # Get field values
            values = self.get_invalid_field(error, schema_full)
            # Get uri of problematic field
            uri = error.json_path.replace("$.meta.", "")
            # Some fixes when error type is "required"
            if error_type == "required":
                rex = r"'(.*)' is a required property"
                uri = f"{uri}.{re.findall(rex, error.message)[0]}"
                if "errorMessage" not in values:
                    values["errorMessage"] = error.message.replace("'", "`")
            # Save error message
            if "errorMessage" in values:
                self.errors[uri] = values["errorMessage"]
            else:
                self.errors[uri] = error.message

    def get_invalid_field(self: Self, error, schema_full) -> Any:
        """Get all key-values for the field that did not validate."""
        queue = list(error.schema_path)[:-1]
        values = schema_full.copy()
        for key in queue:
            values = values[key]
        return values

    def check_required(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are not empty."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            if attr == "":
                self.errors[field_name] = f"`{field_name}` is a required property"

    def check_snake(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are in snake case."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            if not is_snake(attr):
                self.errors[field_name] = f"`{field_name}` must be in snake case"

    def check_is_version(self: Self, fields_names: List[str]) -> None:
        """Check that all fields in `fields_names` are in snake case."""
        for field_name in fields_names:
            attr = getattr(self, field_name)
            rex = r"^\d{4}-\d{2}-\d{2}$|^\d{4}$|^latest$"
            if not re.fullmatch(rex, attr):
                self.errors[field_name] = f"`{field_name}` must have format YYYY-MM-DD, YYYY or 'latest'"


def is_snake(s: str) -> bool:
    """Check that `s` is in snake case.

    First character is not allowed to be a number!
    """
    rex = r"[a-z][a-z0-9]+(?:_[a-z0-9]+)*"
    return bool(re.fullmatch(rex, s))


def extract(error_message: str) -> List[Any]:
    """Get field name that caused the error."""
    rex = r"'(.*)' is a required property"
    return re.findall(rex, error_message)[0]


def config_style_html() -> None:
    st.markdown(
        """
    <style>
    .streamlit-expanderHeader {
        font-size: x-large;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


def preview_file(file_path: str | Path, language: str = "python") -> None:
    """Preview file in streamlit."""
    with open(file_path, "r") as f:
        code = f.read()
    with st.expander(f"File: `{file_path}`", expanded=False):
        st.code(code, language=language)


def preview_dag_additions(dag_content: str, dag_path: str | Path) -> None:
    """Preview DAG additions."""
    if dag_content:
        with st.expander(f"File: `{dag_path}`", expanded=False):
            st.code(dag_content, "yaml")


@st.cache_data
def load_instructions() -> str:
    """Load snapshot step instruction text."""
    with open(CURRENT_DIR / f"{st.session_state['step_name']}.md", "r") as f:
        return f.read()


def _check_env() -> bool:
    """Check if environment variables are set correctly."""
    ok = True
    for env_name in ("GRAPHER_USER_ID", "DB_USER", "DB_NAME", "DB_HOST"):
        if getattr(config, env_name) is None:
            ok = False
            st.warning(f"Environment variable `{env_name}` not found, do you have it in your `.env` file?")

    if ok:
        st.success(("`.env` configured correctly"))
    return ok


def _check_db() -> bool:
    try:
        with st.spinner():
            _ = get_connection()
    except OperationalError as e:
        st.error(
            "We could not connect to the database. If connecting to a remote database, remember to"
            f" ssh-tunel into it using the appropriate ports and then try again.\n\nError:\n{e}"
        )
        return False
    except Exception as e:
        raise e
    st.success("Connection to the Grapher database was successfull!")
    return True


def _show_environment():
    """Show environment variables."""
    st.info(
        f"""
    **Environment variables**:

    ```
    GRAPHER_USER_ID: {config.GRAPHER_USER_ID}
    DB_USER: {config.DB_USER}
    DB_NAME: {config.DB_NAME}
    DB_HOST: {config.DB_HOST}
    ```
    """
    )


def clean_empty_dict(d: Dict[str, Any]) -> Dict[str, Any] | List[Any]:
    """Remove empty values from dict.

    REference: https://stackoverflow.com/a/27974027/5056599
    """
    if isinstance(d, dict):
        return {k: v for k, v in ((k, clean_empty_dict(v)) for k, v in d.items()) if v}
    if isinstance(d, list):
        return [v for v in map(clean_empty_dict, d) if v]
    return d


def warning_notion_latest() -> None:
    """Show warning on latest metadata definitions being available in Notion."""
    st.warning(
        "Documentation for new metadata is almost complete, but still being finalised. For latest definitions refer to [Notion](https://www.notion.so/owid/Metadata-guidelines-29ca6e19b6f1409ea6826a88dbb18bcc)."
    )