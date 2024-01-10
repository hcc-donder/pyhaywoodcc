# from __future__ import annotations

import itertools

# import collections.abc
from typing import Dict, List

import numpy as np
import pandas as pd
from pycolleague import get_config


def load_config():
    config = get_config()
    return config


def mv_to_delim(
    df: pd.DataFrame,
    keys: List[str] = None,  # type: ignore
    assoc: Dict = {},
    cols: List[str] = None,  # type: ignore
    delim: str = ", ",
) -> pd.DataFrame:
    """
    This converts a multi-valued column into a delimiter-separated column.

    keys (list)        List of all key columns, used to uniquely identify each "row"
    assoc (dict)       Dictionary of associations of multi-valued columns. These
                          columns are collapsed together as a group.
    cols (list)        List of independent multi-valued columns. These columns
                          are collapsed individually.
    delim (str)        The separator to use between values. This defaults to a comma
                          followed by a space.

    That is, it takes this:

    ID         Col1       Col2
    -------    --------   -------
    001        A          1
    001        B          2
    001        C          3

    ...and by invoking it with mv_to_delim(df, keys=['ID'], assoc={'GRP1':['Col1','Col2']}),
        converts it to this:

    ID         Col1       Col2
    -------    --------   -------
    001        A, B, C    1, 2, 3

    """

    # Get all the columns in the original order to restore at the end
    colnames = list(df.columns)

    # Get names of columns used in the associations
    assoc_cols = list(set(itertools.chain(*list(assoc.values())))) if assoc else []

    if keys is None:
        keys = []

    if cols is None:
        cols = []

    # Get list of columns not used at all, and create a dataframe with those columns with the keys,
    #   removing all the rows where all the columns are NAs (these were created for the multi-valued columns)
    unused_cols = list(set(colnames) - set(keys) - set(assoc_cols) - set(cols))
    result_df = df.loc[:, keys + unused_cols].dropna(axis=0, how="all")

    def _process_list(
        df: pd.DataFrame,
        keys: List = None,  # type: ignore
        lst: List = None,  # type: ignore
        delim: str = ", ",
    ):
        if keys is None:
            keys = []

        if lst is None:
            lst = []

        # Replace NAs and NaNs with a string that we can replace later to restore with NaNs
        fillval = "FILLNA__3RvmHNI2SSQSHihTz8Te5JTIdCgjd__ANLLIF"

        # Create a temporary dataframe with just the needed columns and remove the rows where all the columns are NAs
        dfl = df.loc[:, keys + lst].dropna(axis=0, how="all")

        # Fill down the keys
        dfl.loc[:, keys] = dfl.loc[:, keys].ffill()
        dfl = dfl.fillna(fillval)

        # Collapse rows of data grouped by the keys into a delimiter-separated string
        # and then replace the fill string inserted above with NaNs.
        # There are 2 replacements to replace stand-alone fill strings and fill strings
        # that were collapsed into one of the delimiter-separated strings.
        # Could also use the following:
        # return((
        #     dfl
        #     .pivot_table(index=keys, values=lst, aggfunc=lambda x: delim.join(x))
        #     .reset_index()
        #     .replace(fillval, np.nan)
        #     .replace(to_replace=fillval, value='', regex=True)
        # ))
        return (
            dfl.groupby(keys, as_index=False)
            .agg(delim.join)
            .replace(fillval, np.nan)
            .replace(to_replace=fillval, value="", regex=True)
        )

    # Process the associations one by one and add them back into the result df
    if assoc:
        for akey in assoc.keys():
            acols = assoc[akey]

            dfk = _process_list(df, keys, acols, delim)

            # Add these records to the result table
            result_df = pd.merge(result_df, dfk, on=keys, how="left")

    # Process the columns that remain one by one and add them back into the result df
    for col in cols:
        collst = [col]

        dfc = _process_list(df, keys, collst, delim)

        # Add these records to the result table
        result_df = pd.merge(result_df, dfc, on=keys, how="left")

    # Return the result dataframe with the columns in the original order
    return result_df.loc[:, colnames]


def mv_to_commas(
    df: pd.DataFrame,
    keys: List = None,  # type: ignore
    assoc: dict = {},
    cols: List = None,  # type: ignore
) -> pd.DataFrame:
    if keys is None:
        keys = []

    if cols is None:
        cols = []

    return mv_to_delim(df, keys=keys, assoc=assoc, cols=cols, delim=", ")


def delim_to_mv(
    df: pd.DataFrame,
    keys: List = None,  # type: ignore
    cols: List = None,  # type: ignore
    delim: str = ", ",
    fill: bool = True,
) -> pd.DataFrame:
    """
    This converts a delimiter-separated column into a multi-valued column.

    keys (list)        List of all key columns, used to uniquely identify each "row"
    cols (list)        List of independent multi-valued columns. These columns
                          are collapsed individually.
    delim (str)        The separator to use between values. This defaults to a comma
                          followed by a space.
    fill (bool)        Keep column filled with duplicates (True) or replace with NaN (False)
                          [NOT IMPLEMENTED YET]

    That is, it takes this:

    ID         Col1       Col2
    -------    --------   -------
    001        A, B, C    1, 2, 3

    ...and by invoking it with delim_to_mv(df, keys=['ID'], cols=['Col1','Col2']),
        converts it to this:

    ID         Col1       Col2
    -------    --------   -------
    001        A          1
    001        B          2
    001        C          3

    """

    if keys is None:
        keys = []

    if cols is None:
        cols = []

    # Get all the columns in the original order to restore at the end
    colnames = list(df.columns)

    # Get list of columns not used at all, and create a dataframe with those columns with the keys,
    #   removing all the rows where all the columns are NAs (these were created for the multi-valued columns)
    unused_cols = list(set(colnames) - set(keys) - set(cols))
    result_df = df.loc[:, keys + unused_cols]

    build_df = pd.DataFrame()

    for col in cols:
        collst = [col]

        dfc = (
            df.loc[:, keys + collst]
            .set_index(keys)[collst]
            .apply(lambda x: x.split(delim) if type(x) == str else x.str.split(delim))
            .explode(collst)
            # .str
            # .strip()
            .reset_index()
        )

        if build_df.empty:
            build_df = dfc.copy()
        else:
            build_df[collst] = dfc.loc[:, collst]

    result_df = pd.merge(result_df, build_df, on=keys, how="left")

    if not fill:
        pass

    return result_df.loc[:, colnames]


def commas_to_mv(
    df: pd.DataFrame,
    keys: List = None,  # type: ignore
    cols: List = None,  # type: ignore
    fill: bool = True,
) -> pd.DataFrame:
    if keys is None:
        keys = []

    if cols is None:
        cols = []

    return delim_to_mv(df, keys=keys, cols=cols, delim=", ", fill=fill)


if __name__ == "__main__":
    cfg = load_config()

    keys = ["ID"]
    df = pd.DataFrame(
        {
            "ID": ["01", np.nan, "02", "03", np.nan, np.nan],
            "Award": ["PELL", "SCH", "SCH", "GRANT", "PELL", "SCH"],
            "Type": ["FED", "LOC", "LOC", "STA", np.nan, "FED"],
            "Date": [
                "2021-06-01",
                "2021-06-01",
                "2021-06-04",
                "2021-06-02",
                "2021-06-15",
                "2021-07-01",
            ],
            "Index": ["1", np.nan, "2", "3", np.nan, np.nan],
        }
    )

    df_comma = mv_to_commas(df, keys=keys, assoc={"AWARD": ["Award", "Type", "Date"]})
    df_filldown = commas_to_mv(df_comma, keys=keys, cols=["Award", "Type", "Date"])

    print(
        "df:\n",
        df,
        "\n\ndf_comma:\n",
        df_comma,
        "\n\ndf_filldown:\n",
        df_filldown,
        "\n\n",
    )

    print(
        "df:\n",
        df,
        "\n\n",
        mv_to_delim(
            df, keys=keys, assoc={"AWARD": ["Award", "Type", "Date"]}, delim=";;"
        ),
    )

    print("Done.")
