import os.path

# import sys
from typing import List, Union

import duckdb as ddb
import pandas as pd
import polars as pl
from pycolleague import ColleagueConnection

# class IPEDS(object):

#     conn: ColleagueConnection
#     config: Dict[str, Union[str, int, float, bool]]

#     def __init__(self, conn: ColleagueConnection):


# Define a local versionof the ColleagueConnection class to use for this function.
# Need to do this because we need to override the default values for df_format and lazy.
class LocalConnection(ColleagueConnection):
    pass


# Get the terms and report_terms data frames
def get_terms(
    lconn: LocalConnection,
    report_years: Union[int, List[int], None] = None,
    report_semesters: Union[str, List[str], None] = None,
) -> List[Union[Union[pl.DataFrame, pl.LazyFrame], Union[pl.DataFrame, pl.LazyFrame]]]:
    terms = (
        pl.DataFrame(
            lconn.get_data(
                "Term_CU",
                schema="dw_dim",
                cols={
                    "Term_ID": "Term_ID",
                    "Term_Index": "Term_Index",
                    "Semester": "Term_Name",
                    "Term_Abbreviation": "Semester",
                    "Term_Start_Date": "Term_Start_Date",
                    "Term_Census_Date": "Term_Census_Date",
                    "Term_End_Date": "Term_End_Date",
                    "Reporting_Year_FSS": "Term_Reporting_Year",
                    "Reporting_Academic_Year_FSS": "Academic_Year",
                },
            )
        )
        .cast(
            {
                # Convert dates to polars datetime type
                "Term_Start_Date": pl.Date,
                "Term_Census_Date": pl.Date,
                "Term_End_Date": pl.Date,
                "Term_Reporting_Year": pl.Int64,
                "Term_Index": pl.Int64,
            }
        )
        # subtract 1 from Term_Reporting_Year to reflect the fall term year
        .with_columns(
            Term_Reporting_Year=pl.col("Term_Reporting_Year") - 1,
        )
    )

    if report_years is None:
        reporting_terms = terms
    else:
        # If report_years is a single year, convert to list
        # Use isinstance to check for int or list

        if isinstance(report_years, int):
            report_years = [report_years]

        reporting_terms = terms.filter(
            pl.col("Term_Reporting_Year").is_in(report_years)
        )

    if report_semesters is not None:
        if isinstance(report_semesters, str):
            report_semesters = [report_semesters]

        reporting_terms = reporting_terms.filter(
            pl.col("Semester").is_in(report_semesters)
        )

    # reporting_terms = reporting_terms.collect()

    return [terms, reporting_terms]


#' Return enrollment for specified term as of the IPEDS reporting date of October 15
#'
#' All data comes from CCDW_HIST SQL Server database
#'
#' @param report_years The ending year of the academic year of the data
#' @param report_semesters Either a single semester abbreviation or a list of semester abbreviations. If unspecified, all semesters are returned.
#' @export
#' @importFrom ccdwr getColleagueData
#' @importFrom magrittr %>%
#' @importFrom dplyr select collect mutate filter inner_join left_join
#'     group_by summarise distinct anti_join ungroup coalesce
#' @importFrom stringr str_c
#'
def term_enrollment(
    conn: ColleagueConnection,
    report_years: Union[int, List[int], None] = None,
    report_semesters: Union[str, List[str], None] = None,
) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]:
    """
    Return enrollment for specified term as of the IPEDS reporting date of October 15

    Args:
        conn: A ColleagueConnection object
        report_years: The list of years to include in the data. If unspecified, all years are returned.
        report_semesters: Either a single semester abbreviation or a list of semester abbreviations. If unspecified, all semesters are returned.

    Returns:
        A pandas or polars dataframe of the data
    """
    source: str = conn.source
    df_format: str = conn.df_format
    lazy: bool = conn.lazy

    # lconn = local_conn_type(source=source, df_format=df_format, lazy=lazy)
    lconn = LocalConnection(
        source=conn.source,
        sourcepath=conn.sourcepath,
        format="polars",
        # lazy=True,
        lazy=False,
        config=conn.config,
        read_only=conn.read_only,
    )

    terms, reporting_terms = get_terms(
        lconn, report_years=report_years, report_semesters=report_semesters
    )

    # Need to get section location for distance learning courses
    # Right now, just take most recent. Probably need to do this the same way as SAC below.
    course_sections = pl.DataFrame(
        lconn.get_data(
            "COURSE_SECTIONS",
            cols={
                "COURSE.SECTIONS.ID": "Course_Section_ID",
                "SEC.TERM": "Term_ID",
                "SEC.LOCATION": "Section_Location",
                "X.SEC.DELIVERY.METHOD": "Delivery_Method",
                "X.SEC.DELIVERY.MODE": "Delivery_Mode",
                # "X.SEC.DELIVERY.NCIH.FLAG" : "Delivery_NCIH_Flag",
                # "X.SEC.DELIVERY/MODIFIER" : "Delivery_Modifier",
            },
        )
    )

    student_acad_cred = (
        pl.DataFrame(
            lconn.get_data(
                "STUDENT_ACAD_CRED",
                version="history",
                cols={
                    "STC.PERSON.ID": "Person_ID",
                    "STC.TERM": "Term_ID",
                    "STUDENT.ACAD.CRED.ID": "Course_ID",
                    "STC.CRED": "Credit",
                    "STC.COURSE.LEVEL": "Course_Level",
                    "STC.VERIFIED.GRADE": "Grade_Code",
                    "STC.SECTION.NO": "Course_Section",
                    "STC.COURSE.SECTION": "Course_Section_ID",
                    "STC.STATUS": "Course_Status",
                    "EffectiveDatetime": "EffectiveDatetime",
                },
                where="""
                    [STC.CRED] > 0
                    AND [STC.ACAD.LEVEL] == 'CU'
                    /*AND [STC.PERSON.ID] IN ['0078937','1151394']
                    AND [STC.TERM] IN ('2022FA')*/
                """,
                # debug="query",
            )
        )
        .cast(
            {
                # Convert EffectiveDatetime to polars datetime type
                "EffectiveDatetime": pl.Datetime,
                # Convert Credit to numeric value
                "Credit": pl.Int32,
            }
        )
        .join(
            pl.DataFrame(
                terms.select(
                    ["Term_ID", "Term_Reporting_Year", "Semester", "Term_Census_Date"]
                )
            ),
            on="Term_ID",
            how="inner",
        )
        .with_columns(
            # Set Keep_FA to True when Semester is FA and EffectiveDatetime is on or before October 15 of the Term_Reporting_Year
            # Set Keep_NF to True when Semester is not FA
            # Use datetime with time set to 23:59:59 to make sure we get all the courses for the day
            # Oct15dt=pl.datetime(
            #     year=pl.col("Term_Reporting_Year"),
            #     month=pl.lit("10"),
            #     day=pl.lit("15"),
            #     hour=pl.lit("23"),
            #     minute=pl.lit("59"),
            #     second=pl.lit("59"),
            # ),
            # Use date, which sets the time to 00:00:00, which will not select any courses on Oct 15
            Oct15=pl.date(
                year=pl.col("Term_Reporting_Year"), month=pl.lit("10"), day=pl.lit("15")
            ),
        )
        .with_columns(
            Keep_FA=(
                (pl.col("Semester") == "FA")
                & (pl.col("EffectiveDatetime") <= pl.col("Oct15"))
            ),
            Keep_NF=(pl.col("Semester") != "FA"),
        )
        .filter(pl.col("Keep_FA") | pl.col("Keep_NF"))
        .drop(["Oct15", "Keep_FA", "Keep_NF"])
        .join(
            course_sections,
            on=["Term_ID", "Course_Section_ID"],
            how="left",
        )
    )

    #
    # Get most recent effective date for each person for each term for each course
    #
    sac_max_effdt = (
        student_acad_cred.group_by(["Person_ID", "Term_ID", "Course_ID"]).agg(
            pl.max("EffectiveDatetime").alias("EffectiveDatetime")
        )
        # .collect()
    )

    #
    # Now get the course data for the latest courses.
    # Use Status of A,N for FA since we want only enrolled courses at the cutoff date
    #     (This will be taken care of later as we need the W credits to determine load)
    # Use Status A,N,W for SP,SU since these were all the courses enrolled in at census
    #
    sac_most_recent_all = (
        student_acad_cred.join(
            sac_max_effdt,
            on=["Person_ID", "Term_ID", "Course_ID", "EffectiveDatetime"],
            how="inner",
        )
        .filter(pl.col("Course_Status").is_in(["A", "N", "W"]))
        .drop(["EffectiveDatetime"])
        .unique()
        .drop(["Course_ID"])
        # .collect()
    )

    #
    # Get list of students who are taking at least 1 non-developmental/audited course
    #
    sac_most_recent_non_dev_ids = (
        sac_most_recent_all.filter(
            pl.col("Course_Level").fill_null("ZZZ") != "DEV",
            pl.col("Grade_Code").fill_null("X") != "9",
        )
        .select(["Person_ID", "Term_ID"])
        .unique()
        # .collect()
    )

    #
    # Get list of students who are taking at least 1 distance course
    #
    sac_most_recent_1_distance_ids = (
        sac_most_recent_all.join(
            sac_most_recent_non_dev_ids,
            on=["Person_ID", "Term_ID"],
            how="inner",
        )
        .filter(
            pl.col("Delivery_Method") == "IN",
            pl.col("Grade_Code").fill_null("X") != "9",
        )
        .select(["Person_ID", "Term_ID"])
        .unique()
        # .collect()
    )

    #
    # Get list of students who are taking at least 1 regular course
    #
    sac_most_recent_f2f_ids = (
        sac_most_recent_all.filter(
            pl.col("Delivery_Method") != "IN",
            pl.col("Grade_Code").fill_null("X") != "9",
        )
        .select(["Person_ID", "Term_ID"])
        .unique()
        # .collect()
    )

    sac_most_recent_all_distance_ids = sac_most_recent_1_distance_ids.join(
        sac_most_recent_f2f_ids,
        on=["Person_ID", "Term_ID"],
        how="anti",
    ).with_columns(Distance_Courses=pl.lit("All"))

    sac_most_recent_distance_ids = sac_most_recent_1_distance_ids.join(
        sac_most_recent_all_distance_ids,
        on=["Person_ID", "Term_ID"],
        how="left",
    ).with_columns(pl.col("Distance_Courses").fill_null(pl.lit("At least 1")))

    # Determine which students have completely withdrawn at the end or by Oct 15
    sac_most_recent_all_withdraws = (
        sac_most_recent_all.filter(pl.col("Course_Status") == "W")
        .join(
            sac_most_recent_all.filter(pl.col("Course_Status").is_in(["A", "N"])),
            on=["Person_ID", "Term_ID"],
            how="anti",
        )
        .select(["Person_ID", "Term_ID"])
        .unique()
        .with_columns(Enrollment_Status=pl.lit("Withdrawn"))
        # .collect()
    )

    #
    # Now create a summary table to calculate load by term
    #
    sac_load_by_term = (
        sac_most_recent_all.join(
            sac_most_recent_non_dev_ids,
            on=["Person_ID", "Term_ID"],
            how="inner",
        )
        .join(
            pl.DataFrame(terms.select(["Term_ID", "Term_Reporting_Year", "Semester"])),
            on=["Term_ID", "Term_Reporting_Year", "Semester"],
            how="inner",
        )
        .group_by(["Person_ID", "Term_ID", "Term_Reporting_Year", "Semester"])
        .agg(pl.sum("Credit").alias("Credits"))
        .with_columns(
            Status=pl.when(pl.col("Credits") >= 12)
            .then(pl.lit("FT"))
            .otherwise(pl.lit("PT"))
        )
        .join(sac_most_recent_distance_ids, on=["Person_ID", "Term_ID"], how="left")
        .join(sac_most_recent_all_withdraws, on=["Person_ID", "Term_ID"], how="left")
        .with_columns(
            pl.col("Distance_Courses").fill_null(pl.lit("None")),
            pl.col("Enrollment_Status").fill_null(pl.lit("Enrolled")),
        )
    )

    #
    # Take term load table and reduce to the reporting terms
    #
    sac_load_by_term = sac_load_by_term.join(
        pl.DataFrame(reporting_terms.select(["Term_ID", "Term_Reporting_Year"])),
        on=["Term_ID", "Term_Reporting_Year"],
        how="inner",
    )

    if conn.df_format == "pandas":
        # We need to return a pandas dataframe

        # If the local connection is lazy, we need to collect the data
        #     since pandas dataframes are not lazy
        if lconn.lazy:
            sac_load_by_term = sac_load_by_term.collect()

        # Now convert to pandas dataframe
        sac_load_by_term = sac_load_by_term.to_pandas()

    else:  # df_format is polars
        # Since the return format is polars, we just need to check the lazy flag

        # If the return connection is lazy...
        if conn.lazy:
            # ...and the local connection is not lazy, we need to convert to a lazy dataframe
            if not lconn.lazy:
                sac_load_by_term = sac_load_by_term.lazy()
        else:  # The return connection is not lazy
            # ...and the local connection is lazy, we need to collect the data
            if lconn.lazy:
                sac_load_by_term = sac_load_by_term.collect()

    return sac_load_by_term


#' A special function to call term_enrollment for just a fall term
#'
#' All data comes from CCDW_HIST SQL Server database
#'
#' @param report_years The year of the fall term for the data
#' @export
#'
def fall_enrollment(
    conn: ColleagueConnection, report_years: Union[int, List[int], None] = None
):
    return term_enrollment(conn, report_years, "FA")


#' Return a data frame of students who are curriculum credential seekers (seeking an Associate's, Diploma, or Certificate)
#'
#' All data comes from CCDW_HIST SQL Server database
#'
#' @param report_years The year or a list of years of the fall term for the data. If unspecified, all years are returned.
#' @param report_semesters Either a single semester abbreviation or a list of semester abbreviations. If unspecified, all semesters are returned.
#' @param exclude_hs Should function exclude high school students from being included as credential seekers. Default is to include high school students.
#' @export
#' @importFrom ccdwr getColleagueData
#' @importFrom magrittr %<>% %>%
#' @importFrom dplyr select collect mutate filter inner_join anti_join
#'     full_join left_join distinct case_when coalesce
#'
def credential_seekers(
    conn: ColleagueConnection,
    report_years: Union[int, List[int], None] = None,
    report_semesters: Union[str, List[str], None] = None,
    exclude_hs: bool = False,
):
    lconn = LocalConnection(
        source=conn.source,
        sourcepath=conn.sourcepath,
        format="polars",
        # lazy=True,
        lazy=False,
        config=conn.config,
        read_only=conn.read_only,
    )

    terms, reporting_terms = get_terms(
        lconn, report_years=report_years, report_semesters=report_semesters
    )

    # Get only CU programs from ACAD_PROGRAMS
    acad_programs = pl.DataFrame(
        lconn.get_data(
            "ACAD_PROGRAMS",
            cols={"ACAD.PROGRAMS.ID": "Program"},
            where="[ACPG.ACAD.LEVEL] == 'CU'",
        )
    )

    # Get earliest start date from the reporting terms as YYYY-MM-DD
    report_term_start_date = (
        reporting_terms.select("Term_Census_Date").min().rows()[0][0]
    ).strftime("%Y-%m-%d")

    hs_students__all = pl.DataFrame(
        lconn.get_data(
            "STUDENTS__STU_TYPES",
            version="history",
            cols={
                "STUDENTS.ID": "Person_ID",
                "STU.TYPES": "Student_Type",
                "STU.TYPE.DATES": "Student_Type_Date",
                "STU.TYPE.END.DATES": "Student_Type_End_Date",
            },
            where=f"""
                [STU.TYPES] IN ['HUSK','DUAL','CCPP','ECOL']
                AND [STU.TYPE.END.DATES] >= '{report_term_start_date}' 
            """,
            # debug="query",
        )
    ).cast(
        {
            "Student_Type_Date": pl.Date,
            "Student_Type_End_Date": pl.Date,
        }
    )

    hs_students = ddb.sql(
        """
        SELECT DISTINCT 
               hs.Person_ID
             , hs.Student_Type
             , rt.Term_ID
        FROM hs_students__all hs
        CROSS JOIN (SELECT Term_ID, Term_Census_Date FROM reporting_terms) rt
        WHERE hs.Student_Type_Date <= rt.Term_Census_Date
        AND hs.Student_Type_End_Date >= rt.Term_Census_Date
        ORDER BY hs.Person_ID, rt.Term_ID
        """
    ).pl()

    #
    # Get program dates (this is a multi-valued field that needs to be joined with full table).
    #
    student_programs__dates = (
        pl.DataFrame(
            lconn.get_data(
                "STUDENT_PROGRAMS__STPR_DATES",
                version="history",
                cols={
                    "STPR.STUDENT": "Person_ID",
                    "STPR.ACAD.PROGRAM": "Program",
                    "STPR.START.DATE": "Program_Start_Date",
                    "STPR.END.DATE": "Program_End_Date",
                    # "EffectiveDatetime": "EffectiveDatetime",
                },
            )
        )
        .with_columns(
            Program_Start_Date=pl.col("Program_Start_Date").cast(pl.Date),
            Program_End_Date=pl.col("Program_End_Date").cast(pl.Date),
            # EffectiveDatetime=pl.col("EffectiveDatetime").cast(pl.Date),
        )
        .join(
            acad_programs,
            on="Program",
            how="inner",
        )
        .with_columns(
            Program_End_Date=pl.col("Program_End_Date").fill_null(pl.date(9999, 12, 31))
        )
        .unique()
    )

    # if exclude_hs:
    #     student_programs__dates = student_programs__dates.join(
    #         hs_students_ids,
    #         on=["Person_ID"],
    #         how="anti",
    #     )

    #
    # Credential-seekers are those in A, D, or C programs
    #
    credential_seeking = (
        student_programs__dates
        # .with_columns(
        #     EffectiveDatetime=pl.col("EffectiveDatetime").cast(pl.Date),
        # )
        # .join(
        #     acad_programs,
        #     on="Program",
        #     how="inner",
        # )
        # .join(
        #     student_programs__dates,
        #     on=["Person_ID", "Program", "EffectiveDatetime"],
        #     how="inner",
        # )
        # .drop("EffectiveDatetime")
        # Identify credential seekers.
        .with_columns(
            Credential_Seeker=pl.when(
                pl.col("Program").str.contains("^(A|D|C)"),
            )
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
        )
        # Cross join with terms to get all the terms they were enrolled in this credential program.
        .join(
            pl.DataFrame(
                terms.select(
                    ["Term_ID", "Term_Start_Date", "Term_Census_Date", "Term_End_Date"]
                )
            ),
            how="cross",
        )
        .filter(
            pl.col("Program_Start_Date") <= pl.col("Term_Census_Date"),
            pl.col("Program_End_Date") >= pl.col("Term_Census_Date"),
            pl.col("Credential_Seeker") > 0,
        )
        .join(
            pl.DataFrame(reporting_terms.select(["Term_ID"])),
            on=["Term_ID"],
            how="inner",
        )
        .join(
            hs_students,
            on=["Person_ID", "Term_ID"],
            how="left",
        )
        .with_columns(
            hs_student=(pl.col("Student_Type").fill_null("") != ""),
        )
        .filter((exclude_hs & ~pl.col("hs_student")) | (exclude_hs is False))
        .select(
            [
                "Person_ID",
                "Term_ID",
                "Credential_Seeker",
            ]
        )
        .unique()
        # .collect()
    )

    if conn.df_format == "pandas":
        # We need to return a pandas dataframe

        # If the local connection is lazy, we need to collect the data
        #     since pandas dataframes are not lazy
        if lconn.lazy:
            credential_seeking = credential_seeking.collect()

        # Now convert to pandas dataframe
        credential_seeking = credential_seeking.to_pandas()

    else:  # df_format is polars
        # Since the return format is polars, we just need to check the lazy flag

        # If the return connection is lazy...
        if conn.lazy:
            # ...and the local connection is not lazy, we need to convert to a lazy dataframe
            if not lconn.lazy:
                credential_seeking = credential_seeking.lazy()
        else:  # The return connection is not lazy
            # ...and the local connection is lazy, we need to collect the data
            if lconn.lazy:
                credential_seeking = credential_seeking.collect()

    return credential_seeking


#' A special function to call credential_seekers for just a fall term
#'
#' All data comes from CCDW_HIST SQL Server database
#'
#' @param report_years The year of the fall term for the data
#' @param exclude_hs Should function exclude high school students from being included as credential seekers. Default is to include high school students.
#' @export
#'
def fall_credential_seekers(
    conn: ColleagueConnection,
    report_years: Union[int, List[int], None] = None,
    exclude_hs: bool = False,
):
    return credential_seekers(
        conn, report_years=report_years, report_semesters="FA", exclude_hs=exclude_hs
    )


#' Return a data from of the IPEDS cohort data.
#'
#' Return a data from of the IPEDS cohort data. Data will come either from the file ipeds_cohorts.csv or
#' from the CCDW_HIST SQL Server database.
#'
#' @param report_years The year of the fall term for the data
#' @param cohorts Which cohorts to include in data frame. FT = Full-time First-time, PT = Part-time First-time,
#'                TF = Full-time Transfer, TP = Part-time Transfer,
#'                RF = Full-time Returning, RP = Part-time Returning
#' @param cohort_types Which cohort fields to include in data frame. Default is Cohort only. Choose from
#'                      "Cohort", "OM_Cohort", "Term_Cohort".
#' @param use Which dataset should be used for cohorts from Colleague
#'            ipeds_cohorts Use the local.ipeds_cohorts table
#'            STUDENT_TERMS Use the history.STUDENT_TERMS_Current view
#' @param useonly Use the database cohort found in the table specified in the
#'     `use` parameter only. Default is FALSE which means combine data from
#'     the database table with the file ipeds_cohorts.csv.
#' @param ipeds_path The path where ipeds_cohort.csv file is located.
#' @export
#' @importFrom ccdwr getColleagueData
#' @importFrom purrr has_element
#' @importFrom dplyr filter select collect bind_rows distinct mutate
#' @importFrom stringr str_c
#' @importFrom readr read_csv cols col_character
#'
def ipeds_cohort(
    conn: ColleagueConnection,
    report_years: Union[int, List[int], None] = None,
    cohorts: Union[str, List[str]] = ["FT", "PT", "TF", "TP", "RF", "RP"],
    cohort_types: Union[
        str, List[str]
    ] = "Cohort",  # Also allows "OM_Cohort","Term_Cohort"
    use: str = "ipeds_cohorts",
    ipeds_path: str = "",
    useonly: bool = True,
) -> Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]:
    ipeds_cohort: Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame]

    lconn = LocalConnection(
        source=conn.source,
        sourcepath=conn.sourcepath,
        format="polars",
        # lazy=True,
        lazy=False,
        config=conn.config,
        read_only=conn.read_only,
    )

    # Make sure cohort_types is a list
    if isinstance(cohort_types, str):
        cohort_types = [cohort_types]

    valid_cohort_types = ["Cohort", "OM_Cohort", "Term_Cohort"]
    if set(cohort_types).issubset(valid_cohort_types) is False:
        raise ValueError(
            f"Invalid cohort type in cohort_types parameter. Must be one of [{valid_cohort_types}]."
        )

    # Check to see if cohorts only contains valid values
    valid_cohorts = ["FT", "PT", "TF", "TP", "RF", "RP"]
    if set(cohorts).issubset(valid_cohorts) is False:
        raise ValueError(
            f"Invalid cohort value in cohorts parameter. Must be one of [{valid_cohorts}]."
        )

    # Make cohorts a list if it is a single value
    if isinstance(cohorts, str):
        cohorts = [cohorts]

    if ipeds_path == "":
        ipeds_path = "."  # Default to current directory
        # print warning message about using current directory

    # Check to see if ipeds_cohorts.csv file exists in ipeds_path
    if os.path.isfile(os.path.join(ipeds_path, "ipeds_cohorts.csv")):
        # Read the ipeds_cohorts.csv file
        ipeds_cohort_FILE_COHORTS = (
            pl.read_csv(
                os.path.join(ipeds_path, "ipeds_cohorts.csv"),
                dtypes="str",  # {"Cohort": "string", "OM_Cohort": "string", "Term_Cohort": "string"},
            )
            .filter(pl.col("Cohort").is_in(cohorts))
            .select(["Person_ID", "Term_ID", "Cohort"])
        )
    else:
        # If the file does not exist, return an empty data frame
        ipeds_cohort_FILE_COHORTS = pl.DataFrame()

    if use == "STUDENT_TERMS":
        # If use is STUDENT_TERMS, return the STUDENT_TERMS_Current view
        ipeds_cohort = (
            lconn.get_data(
                "STUDENT_TERMS",
                cols={
                    "STTR.STUDENT": "Person_ID",
                    "STTR.FED.COHORT.GROUP": "Cohort",
                },
            )
            .filter(pl.col("Cohort").is_in(cohorts))
            .unique()
            .select(["Person_ID", "Term_ID", "Cohort"])
            .with_columns(
                Term_ID=f'{pl.col("Cohort").str.slice(1,4)}FA',
            )
        )

    else:
        if use != "ipeds_cohorts":
            raise ValueError("Invalid value for use parameter.")

        # If use is ipeds_cohorts, return the ipeds_cohorts.csv file
        ipeds_cohort = lconn.get_data(
            "ipeds_cohorts",
            schema="local",
        ).rename({"ID": "Person_ID"})

        # Select the Person_ID, Term_ID, Cohort columns as well as any columns named in cohort_types
        ipeds_cohort = ipeds_cohort.select(["Person_ID", "Term_ID"] + cohort_types)

        if cohort_types == ["Cohort"]:
            # filter out rows where cohort is null
            ipeds_cohort = ipeds_cohort.filter(pl.col("Cohort").is_not_null())

    # if useonly is False, combine the data from the database with the data from the file
    if useonly is False:
        ipeds_cohort = ipeds_cohort_FILE_COHORTS.vstack(ipeds_cohort)

    if conn.df_format == "pandas":
        # We need to return a pandas dataframe

        # If the local connection is lazy, we need to collect the data
        #     since pandas dataframes are not lazy
        if lconn.lazy:
            ipeds_cohort = ipeds_cohort.collect()

        # Now convert to pandas dataframe
        ipeds_cohort = ipeds_cohort.to_pandas()

    else:  # df_format is polars
        # Since the return format is polars, we just need to check the lazy flag

        # If the return connection is lazy...
        if conn.lazy:
            # ...and the local connection is not lazy, we need to convert to a lazy dataframe
            if not lconn.lazy:
                ipeds_cohort = ipeds_cohort.lazy()
        else:  # The return connection is not lazy
            # ...and the local connection is lazy, we need to collect the data
            if lconn.lazy:
                ipeds_cohort = ipeds_cohort.collect()

    return ipeds_cohort


# For testing purposes only
if __name__ == "__main__":
    import time

    report_semesters = ["FA", "SP", "SU"]
    report_years = [2020, 2021, 2022]

    conn_pd = ColleagueConnection(
        source="ccdw",
        format="pandas",
        lazy=False,
    )
    conn_pl = ColleagueConnection(
        source="ccdw",
        format="polars",
        lazy=False,
    )
    conn_pll = ColleagueConnection(
        source="ccdw",
        format="polars",
        lazy=True,
    )

    # df = load_data("ccp_programs", "pandas")
    # df = load_data("ccp_programs", "polars")
    # df = load_data("ccp_programs", "polars", True)

    total_start_time = time.time()

    print(f"report_years: {report_years}")
    print(f"report_semesters: {report_semesters}")

    # credential_seekers
    start_time = time.time()
    df_pd = credential_seekers(
        conn_pd,
        report_years=report_years,
        report_semesters=report_semesters,
        exclude_hs=True,
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, terms: {df_pd['Term_ID'].unique().tolist()}"
    )

    ### term_enrollment
    print("Run term_enrollment with format=pandas, lazy=False")
    start_time = time.time()
    df_pd = term_enrollment(
        conn_pd, report_years=report_years, report_semesters=report_semesters
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, terms: {df_pd['Term_ID'].unique().tolist()}, years: {df_pd['Term_Reporting_Year'].unique().tolist()}"
    )
    df_pd.sort_values(["Person_ID", "Term_Reporting_Year", "Term_ID"]).to_csv(
        "file_term_enrollment.csv", index=False
    )

    print("Run term_enrollment with format=polars, lazy=False")
    start_time = time.time()
    df_pl = term_enrollment(
        conn_pl, report_years=report_years, report_semesters=report_semesters
    )
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}, years: {df_pl['Term_Reporting_Year'].unique().to_list()}"
    )

    print("Run term_enrollment with format=polars, lazy=True")
    start_time = time.time()
    df_pll = term_enrollment(
        conn_pll, report_years=report_years, report_semesters=report_semesters
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}, years: {df_pl['Term_Reporting_Year'].unique().to_list()}"
    )

    print("Done with term_enrollment")

    ### fall_enrollment
    print("Run fall_enrollment with format=pandas, lazy=False")
    start_time = time.time()
    df_pd = fall_enrollment(conn_pd, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, terms: {df_pd['Term_ID'].unique().tolist()}, years: {df_pd['Term_Reporting_Year'].unique().tolist()}"
    )
    df_pd.sort_values(["Person_ID", "Term_Reporting_Year", "Term_ID"]).to_csv(
        "file_fall_enrollment.csv", index=False
    )

    print("Run fall_enrollment with format=polars, lazy=False")
    start_time = time.time()
    df_pl = fall_enrollment(conn_pl, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}, years: {df_pl['Term_Reporting_Year'].unique().to_list()}"
    )

    print("Run fall_enrollment with format=polars, lazy=True")
    start_time = time.time()
    df_pll = fall_enrollment(conn_pll, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}, years: {df_pl['Term_Reporting_Year'].unique().to_list()}"
    )

    print("Done with fall_enrollment")

    ### credential_seekers
    print("Run credential_seekers with format=pandas, lazy=False")
    start_time = time.time()
    df_pd = credential_seekers(
        conn_pd, report_years=report_years, report_semesters=report_semesters
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, terms: {df_pd['Term_ID'].unique().tolist()}"
    )
    df_pd.sort_values(["Person_ID", "Term_ID"]).to_csv(
        "file_credential_seekers.csv", index=False
    )

    print("Run credential_seekers with format=polars, lazy=False")
    start_time = time.time()
    df_pl = credential_seekers(
        conn_pl, report_years=report_years, report_semesters=report_semesters
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}"
    )

    print("Run credential_seekers with format=polars, lazy=True")
    start_time = time.time()
    df_pll = credential_seekers(
        conn_pll, report_years=report_years, report_semesters=report_semesters
    )
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}"
    )

    print("Done with credential_seekers")

    ### fall_credential_seekers
    print("Run fall_credential_seekers with format=pandas, lazy=False")
    start_time = time.time()
    df_pd = fall_credential_seekers(conn_pd, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, terms: {df_pd['Term_ID'].unique().tolist()}"
    )
    df_pd.sort_values(["Person_ID", "Term_ID"]).to_csv(
        "file_fall_credential_seekers.csv", index=False
    )

    print("Run fall_credential_seekers with format=polars, lazy=False")
    start_time = time.time()
    df_pl = fall_credential_seekers(conn_pl, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}"
    )

    print("Run fall_credential_seekers with format=polars, lazy=True")
    start_time = time.time()
    df_pll = fall_credential_seekers(conn_pll, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, terms: {df_pl['Term_ID'].unique().to_list()}"
    )

    print("Done with fall_credential_seekers")

    ### ipeds_cohort
    # report_years: Union[int, List[int], None] = None,
    # cohorts: Union[str, List[str]] = ["FT", "PT", "TF", "TP", "RF", "RP"],
    # cohort_types: Union[
    #     str, List[str]
    # ] = "Cohort",  # Also allows "OM_Cohort","Term_Cohort"
    # use: str = "ipeds_cohorts",
    # ipeds_path: str = "",
    # useonly: bool = True,

    print("Run ipeds_cohort with format=pandas, lazy=False")
    start_time = time.time()
    df_pd = ipeds_cohort(conn_pd, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pd.shape}, cohorts: {df_pd['Cohort'].unique().tolist()}"
    )
    df_pd.sort_values(["Person_ID", "Cohort"]).to_csv(
        "file_ipeds_cohort.csv", index=False
    )

    print("Run ipeds_cohort with format=polars, lazy=False")
    start_time = time.time()
    df_pl = ipeds_cohort(conn_pl, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, cohorts: {df_pl['Cohort'].unique().to_list()}"
    )

    print("Run ipeds_cohort with format=polars, lazy=True")
    start_time = time.time()
    df_pll = fall_enrollment(conn_pll, report_years=report_years)
    elapsed_time = time.time() - start_time
    print(
        f"   ...[{elapsed_time:.2f} secs] shape: {df_pl.shape}, cohorts: {df_pl['Cohort'].unique().to_list()}"
    )

    print("Done with ipeds_cohort")

    total_elapsed_time = time.time() - total_start_time
    print(f"Done [{total_elapsed_time:.2f} secs]")
