"""General data tools.

Use this module with caution. Functions added here are half-way their final destination: owid-datautils.

When working on a specific project, it is often the case that we may identify functions that can be useful for other projects. These functions
should probably be moved to owid-datautils. However this can be time consuming at the time we are working on the project. Therefore:

- By adding them here we make them available for other projects.
- We have these functions in one place if we ever wanted to move them to owid-datautils.
- Prior to moving them to owid-datautils, we can test and discuss them.

"""
from typing import Any, List, Set, Union

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.gam.api import BSplines, GLMGam


def check_known_columns(df: pd.DataFrame, known_cols: list) -> None:
    """Check that all columns in a dataframe are known and none is missing."""
    unknown_cols = set(df.columns).difference(set(known_cols))
    if len(unknown_cols) > 0:
        raise Exception(f"Unknown column(s) found: {unknown_cols}")

    missing_cols = set(known_cols).difference(set(df.columns))
    if len(missing_cols) > 0:
        raise Exception(f"Previous column(s) missing: {missing_cols}")


def check_values_in_column(df: pd.DataFrame, column_name: str, values_expected: Union[Set[Any], List[Any]]):
    """Check values in a column are as expected.

    It checks both ways:
        - That there are no new and unexpected values (compared to `values_expected`).
        - That all expected values are present in the column (all in `values_expected`).
    """
    if not isinstance(values_expected, set):
        values_expected = set(values_expected)
    ds = df[column_name]
    values_obtained = set(ds)
    if values_unknown := values_obtained.difference(values_expected):
        raise ValueError(f"Values {values_unknown} in column `{column_name}` are new, unsure how to map. Review!")
    if values_missing := values_expected.difference(values_obtained):
        raise ValueError(
            f"Values {values_missing} in column `{column_name}` missing, check if they were removed from source!"
        )


def add_cubic_spline(df: pd.DataFrame, time_col: str, var_col: str) -> pd.DataFrame:
    """
    Add cubic spline interpolation of a given variable to a dataframe.
    """
    gam_model = LinearGAM(s(0))  # 's(0)' specifies the cubic spline basis for the first feature
    gam_model.fit(df[time_col], df[var_col])

    new_X = np.linspace(0, 10, 100)
    predictions = gam_model.predict(new_X)

    # Create spline basis with automatic knot selection
    bs = BSplines(X, degree=[3])  # Degree 3 for cubic spline
    gam_model = GLMGam(y, X, smoother=bs)
    result = gam_model.fit()
