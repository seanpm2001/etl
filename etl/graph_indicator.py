import os
import tempfile
import webbrowser

import click
from owid.catalog import Dataset
from owid.catalog.variables import random_hash

from etl.paths import DATA_DIR


@click.command()
@click.argument("dataset_uri")
@click.argument("table_name")
@click.argument("indicator")
@click.option(
    "--text",
    is_flag=True,
)
@click.option(
    "--auto-open/--skip-auto-open",
    is_flag=True,
    default=True,
    help="Open browser automatically",
)
def cli(dataset_uri: str, table_name: str, indicator: str, text: bool = False, auto_open: bool = True) -> None:
    """
    Generate a HTML file with indicator's processing log shown as a Mermaid diagram.
    """
    ds = Dataset(DATA_DIR / dataset_uri)
    tab = ds[table_name].reset_index()

    if indicator not in tab:
        available_indicators = "\n  ".join(tab.columns)
        raise ValueError(
            f"Indicator {indicator} not found in table {table_name}. Available indicators:\n  {available_indicators}"
        )
    pl = tab[indicator].metadata.processing_log

    pl = _preprocess_log(pl)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as temp:
        mermaid_diagram = "\n".join(_mermaid_diagram(pl))

        if text:
            print(mermaid_diagram)
            return

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
        temp.write(s.encode())
        temp.seek(0)

        if auto_open:
            webbrowser.open("file://" + os.path.realpath(temp.name))


def _mermaid_diagram(pl):
    yield "graph TB;"

    for r in pl:
        for parent in r["parents"]:
            # constant or unknown column, add random hash to get unique vertices
            # if / is in parent, it means it's a path (TODO: make it more robust)
            if "#" not in parent and "/" not in parent:
                parent += f"#{random_hash()}"

            yield f"{parent}[{parent.split('#')[0]}] --> |{r['operation']}| {r['target']}[{r['target'].split('#')[0]}]"


def _preprocess_log(pl):
    # try to merge rename with previous operation
    new_pl = []
    last_r = {}
    seen_r = set()
    for r in pl:
        if str(r) in seen_r:
            continue

        if r["operation"] == "rename":
            if last_r["target"] == r["parents"][0]:
                last_r["target"] = r["target"]
                last_r["variable"] = r["variable"]
                continue

        last_r = r
        new_pl.append(r)
        seen_r.add(str(r))

    return new_pl
