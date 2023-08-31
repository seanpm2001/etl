import json
from typing import Any, Dict, cast

import click
import pandas as pd
import structlog
from owid.catalog import Dataset, DatasetMeta, Origin
from sqlalchemy.engine import Engine

from etl.db import get_engine
from etl.files import yaml_dump
from etl.paths import DATA_DIR, STEP_DIR

log = structlog.get_logger()


@click.command()
@click.argument("uri", type=str)
@click.option(
    "--cols",
    type=str,
    help="Only generate metadata for columns matching pattern. ",
)
def cli(
    uri: str,
    cols: str,
) -> None:
    """Generate grapher metadata YAML. Missing mandatory fields are filled with TBD. These
    should be filled manually or semi-automatically with ChatGPT.

    - fill mandatory fields with TBD
    """
    engine = get_engine()

    ds = Dataset(DATA_DIR / "garden" / uri)

    assert not ds.metadata.origins
    assert len(ds.table_names) == 1, "Only one table per dataset supported"
    table_name = ds.table_names[0]

    if len(ds.metadata.licenses) > 1:
        raise NotImplementedError("Multiple licenses not supported")
    elif len(ds.metadata.licenses) == 1:
        license = ds.metadata.licenses[0]
    else:
        license = None

    assert len(ds.metadata.sources) == 1
    source = ds.metadata.sources[0]

    # TODO: check that sources of all indicators is the same

    origin = Origin(
        dataset_title_owid=ds.metadata.title,
        dataset_title_producer=source.name,
        producer="TBD",
        citation_producer=source.published_by,
        license=license,
        dataset_description_owid=ds.metadata.description,
        dataset_description_producer=source.description,
        dataset_url_main=source.url,
        dataset_url_download=source.source_data_url,
        date_accessed=source.date_accessed,
        date_published=source.publication_date or source.publication_year,
    )

    if not origin.date_published:
        log.warning(
            f"missing publication_date and publication_year in source, using date_accessed: {origin.date_accessed}"
        )
        origin.date_published = origin.date_accessed

    tab = ds[table_name]

    vars = {}
    for col in tab.filter(regex=cols):
        vars[col] = {}

        # duplicate metadata from garden YAML, this adds redundancy, but it's easier to fill out the rest of the fields
        # with all metadata in front of us
        var_meta = tab[col].metadata
        for field in ("unit", "title", "description", "short_unit", "display"):
            if getattr(var_meta, field):
                vars[col][field] = getattr(var_meta, field)

        # load the first chart if exists and use its grapher_config
        grapher_config = _load_grapher_config(engine, col, ds.metadata)

        if grapher_config:
            vars[col] = {"presentation": {"grapher_config": grapher_config}}

        vars[col]["sources"] = []
        vars[col]["origins"] = "*origins"

    meta = {"origins": [origin.to_dict()], "tables": {ds.table_names[0]: {"variables": vars}}}

    s = cast(str, yaml_dump(meta))

    # properly format YAML anchors
    s = s.replace("'*origins'", "*origins").replace("origins:\n", "origins: &origins\n")

    with open(STEP_DIR / "data/grapher" / (uri + ".meta.generated.yml"), "w") as f:
        f.write(s)


def _load_grapher_config(engine: Engine, col: str, ds_meta: DatasetMeta) -> Dict[Any, Any]:
    """TODO: This is work in progress! Update this function as you like."""
    q = f"""
    select
        c.config
    from variables as v
    join datasets as d on v.datasetId = d.id
    join chart_dimensions as cd on v.id = cd.variableId
    join charts as c on cd.chartId = c.id
    where
        v.shortName = '{col}' and
        d.namespace = '{ds_meta.namespace}' and
        d.version = '{ds_meta.version}' and
        d.shortName = '{ds_meta.short_name}'
    """
    cf = pd.read_sql(q, engine)
    if len(cf) == 0:
        log.warning(f"no chart found for variable {col}")
        return {}
    # TODO: be smarter about more than one chart and merge them
    # we could even use git pattern to leverage VSCode merging features
    # <<<<<<< chart1
    # foo
    # =======
    # bar
    # >>>>>>> chart2
    elif len(cf) > 1:
        log.warning(f"multiple charts found for variable {col}, using the first one")

    config = json.loads(cf.iloc[0]["config"])

    # prune fields not useful for ETL grapher_config
    # TODO: should we delete title or can we reuse it?
    # TODO: are dimensions useful?
    for field in ("id", "slug", "version", "title", "dimensions", "isPublished"):
        config.pop(field, None)

    # TODO: move subtitle to indicator.description_short

    return config


if __name__ == "__main__":
    cli()
