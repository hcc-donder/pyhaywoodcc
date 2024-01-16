import pkgutil
from pathlib import Path
from typing import List, Optional, Union

import duckdb as ddb
import pandas as pd
import polars as pl
from pycolleague import ColleagueConnection


def load_data(
    dataset: str = "",
    format: str = "pandas",
    lazy: bool = False,
) -> List[Union[Union[pl.DataFrame, pl.LazyFrame], Union[pl.DataFrame, pl.LazyFrame]]]:
    """
    Loads the data from the package into a pandas dataframe
    """
    # Get the path to the data file

    if format == "pandas":
        lazy = False

    # Check if dataset is a valid selection
    if dataset not in [
        "ccp_programs",
        "early_college_programs",
        "haywood_county_high_schools",
        "high_school_programs",
    ]:
        raise ValueError(
            "dataset must be one of ccp_programs, early_college_programs, haywood_county_high_schools, or high_school_programs"
        )

    data = pkgutil.get_data(__package__, "data/ccp_programs.csv")
    return df
