import os
import time

from census import Census
import pandas as pd
from us import states
import requests



def _build_moe_variables(variables):
    moe_vars = []
    for var in variables:
        if var.endswith('E') and var != 'NAME':
            moe_vars.append(f"{var[:-1]}M")
    return moe_vars


def _add_moe_renames(rename_columns):
    moe_mapping = {}
    for var, renamed in rename_columns.items():
        if var.endswith('E') and var != 'NAME':
            moe_mapping[f"{var[:-1]}M"] = f"{renamed}_moe"
    return {**rename_columns, **moe_mapping}


def fetch_census_data(
    census_obj,
    geography,
    years,
    variables,
    rename_columns=None,
    rate_limit_sleep=0.0,
    max_retries=3,
    backoff_seconds=1.0,
    include_moe=False,
):
    """
    Fetch census data for metropolitan statistical areas or counties for multiple years.
    Returns a DataFrame with renamed columns. Uses ACS 1-year estimates.
    
    Arguments:
        census_obj: census API object
        geography: 'msa' or 'county'
        years: list of years to fetch data for
        variables: census variable codes
        rename_columns: optional dict to rename columns. If None, uses default mapping.
        include_moe: if True, also fetch MOE variables and add *_moe columns
    
    Returns:
        pandas DataFrame with census data and renamed columns
    """
    # Default column mappings
    msa_mapping = {
        'NAME': 'name',
        'B01003_001E': 'population',
        'B11001_002E': 'total_families',
        'B11005_003E': 'family_households_with_child',
        'B01002_001E': 'median_age',
        'B09001_001E': 'population_under_18',
        'B07003_004E': 'lived_in_same_house',
        'B07003_007E': 'moved_within_county',
        'B07003_010E': 'moved_from_diff_county_same_state',
        'B07003_013E': 'moved_from_diff_state',
        'B07003_016E': 'moved_from_abroad',
        'metropolitan statistical area/micropolitan statistical area': 'msa_code'
    }
    
    county_mapping = {
        'NAME': 'name',
        'B01003_001E': 'population',
        'B11001_002E': 'total_families',
        'B11005_003E': 'family_households_with_child',
        'B01002_001E': 'median_age',
        'B09001_001E': 'population_under_18',
        'B07003_004E': 'lived_in_same_house',
        'B07003_007E': 'moved_within_county',
        'B07003_010E': 'moved_from_diff_county_same_state',
        'B07003_013E': 'moved_from_diff_state',
        'B07003_016E': 'moved_from_abroad',
        'county': 'county_code'
    }
    
    all_data = []
    
    moe_vars = _build_moe_variables(variables) if include_moe else []
    variables_to_fetch = list(dict.fromkeys([*variables, *moe_vars]))

    for year in years:
        attempt = 0
        while attempt < max_retries:
            try:
                if geography == 'msa':
                    # if geography is MSA, fetch census data for all MSAs
                    data = census_obj.acs1.get(
                        variables_to_fetch,
                        geo={'for': 'metropolitan statistical area/micropolitan statistical area:*'},
                        year=year
                    )
                elif geography == 'county':
                    # if geography is county, fetch census data for all counties in Texas
                    data = census_obj.acs1.state_county(
                        variables_to_fetch,
                        states.TX.fips,
                        Census.ALL,
                        year=year
                    )
                else:
                    raise ValueError(f"geography must be 'msa' or 'county', got '{geography}'")
                
                # Add year to each record
                for record in data:
                    record['year'] = year
                    all_data.append(record)
                
                print(f"Retrieved {geography} data for {year}: {len(data)} records")
                break
            except Exception as e:
                attempt += 1
                if attempt >= max_retries:
                    print(f"Error for year {year}: {e}")
                else:
                    time.sleep(backoff_seconds * attempt)
        if rate_limit_sleep:
            time.sleep(rate_limit_sleep)
    
    # convert to df
    df = pd.DataFrame(all_data)

    # replace ACS missing value sentinels
    df = df.replace(
        {
            -666666666: pd.NA,
            -555555555: pd.NA,
            -222222222: pd.NA,
        }
    )
    
    # use default
    if rename_columns is None:
        rename_columns = msa_mapping if geography == 'msa' else county_mapping

    if include_moe:
        rename_columns = _add_moe_renames(rename_columns)
    
    # rename columns so they are readable
    df = df.rename(columns=rename_columns)
    
    return df


def fetch_census_data_msa_acs5(
    census_obj,
    geography,
    years,
    variables,
    rename_columns=None,
    rate_limit_sleep=0.0,
    max_retries=3,
    backoff_seconds=1.0,
    include_moe=False,
):
    """
    Fetch census data for metropolitan statistical areas or counties for multiple years.
    Returns a DataFrame with renamed columns. Uses ACS 5-year estimates.
    
    Arguments:
        census_obj: census API object
        geography: 'msa' or 'county'
        years: list of years to fetch data for
        variables: census variable codes
        rename_columns: optional dict to rename columns. If None, uses default mapping.
        include_moe: if True, also fetch MOE variables and add *_moe columns
    
    Returns:
        pandas DataFrame with census data and renamed columns
    """
    # Default column mappings
    msa_mapping = {
        'NAME': 'name',
        'B01003_001E': 'population',
        'B11001_002E': 'total_families',
        'B11005_003E': 'family_households_with_child',
        'B01002_001E': 'median_age',
        'B09001_001E': 'population_under_18',
        'B07003_004E': 'lived_in_same_house',
        'B07003_007E': 'moved_within_county',
        'B07003_010E': 'moved_from_diff_county_same_state',
        'B07003_013E': 'moved_from_diff_state',
        'B07003_016E': 'moved_from_abroad',
        'metropolitan statistical area/micropolitan statistical area': 'msa_code'
    }

    county_mapping = {
        'NAME': 'name',
        'B01003_001E': 'population',
        'B11001_002E': 'total_families',
        'B11005_003E': 'family_households_with_child',
        'B01002_001E': 'median_age',
        'B09001_001E': 'population_under_18',
        'B07003_004E': 'lived_in_same_house',
        'B07003_007E': 'moved_within_county',
        'B07003_010E': 'moved_from_diff_county_same_state',
        'B07003_013E': 'moved_from_diff_state',
        'B07003_016E': 'moved_from_abroad',
        'county': 'county_code'
    }
    
    all_data = []
    
    moe_vars = _build_moe_variables(variables) if include_moe else []
    variables_to_fetch = list(dict.fromkeys([*variables, *moe_vars]))

    for year in years:
        attempt = 0
        while attempt < max_retries:
            try:
                if geography == 'msa':
                    # if geography is MSA, fetch census data for all MSAs using ACS 5-year
                    data = census_obj.acs5.get(
                        variables_to_fetch,
                        geo={'for': 'metropolitan statistical area/micropolitan statistical area:*'},
                        year=year
                    )
                elif geography == 'county':
                    # if geography is county, fetch census data for all counties in Texas using ACS 5-year
                    data = census_obj.acs5.state_county(
                        variables_to_fetch,
                        states.TX.fips,
                        Census.ALL,
                        year=year
                    )
                else:
                    raise ValueError(f"geography must be 'msa' or 'county', got '{geography}'")
                
                # Add year to each record
                for record in data:
                    record['year'] = year
                    all_data.append(record)
                
                print(f"Retrieved {geography} data for {year} (ACS5): {len(data)} records")
                break
            except Exception as e:
                attempt += 1
                if attempt >= max_retries:
                    print(f"Error for year {year}: {e}")
                else:
                    time.sleep(backoff_seconds * attempt)
        if rate_limit_sleep:
            time.sleep(rate_limit_sleep)
    
    # convert to df
    df = pd.DataFrame(all_data)

    # replace ACS missing value sentinels
    df = df.replace(
        {
            -666666666: pd.NA,
            -555555555: pd.NA,
            -222222222: pd.NA,
        }
    )
    
    # use default
    if rename_columns is None:
        rename_columns = msa_mapping if geography == 'msa' else county_mapping

    if include_moe:
        rename_columns = _add_moe_renames(rename_columns)
    
    # rename columns so they are readable
    df = df.rename(columns=rename_columns)
    
    return df

API_KEY = os.getenv("CENSUS_API_KEY", "8149a6071641f66c9a2eaa2a679734a3d2a148e1")

census_variables = (
    'NAME', 
    'B01003_001E',  # total population
    'B11001_002E',  # total families
    # 'B11003_004E',  # families with only children under 6
    # 'B11003_005E',  # families with children under 6 AND children 6-17
    'B11005_003E',  # family households with one or more people under 18
    'B01002_001E',  # median age
    'B09001_001E',  # population under 18
    'B07003_004E',  # lived in the same house 1 year ago
    'B07003_007E',  # moved within same county
    'B07003_010E',  # moved from a different county in the same state
    'B07003_013E',  # moved from a different state
    'B07003_016E',  # moved from abroad
)

def fetch_acs5_counties_tx(year, variables=None, include_moe=False, max_retries=3, backoff_seconds=1.0):
    url = f"https://api.census.gov/data/{year}/acs/acs5"

    variables = variables or census_variables
    moe_vars = _build_moe_variables(variables) if include_moe else []
    variables_to_fetch = list(dict.fromkeys([*variables, *moe_vars]))

    params = {
        "get": ",".join(variables_to_fetch),
        "for": "county:*",
        "in": "state:48",  # Texas
        "key": API_KEY
    }
    
    attempt = 0
    while attempt < max_retries:
        try:
            r = requests.get(url, params=params)
            r.raise_for_status()
            
            data = r.json()
            df = pd.DataFrame(data[1:], columns=data[0])

            # replace ACS missing value
            df = df.replace(
                {
                    -666666666: pd.NA,
                    -555555555: pd.NA,
                    -222222222: pd.NA,
                }
            )
            df["year"] = year
            
            return df
        except Exception:
            attempt += 1
            if attempt >= max_retries:
                raise
            time.sleep(backoff_seconds * attempt)

def create_pivot_tables(df, index_cols, variable_names):
    """
    pivot dataframe for specific metrics
    
    Args:
        df: dataframe 
        index_cols: List of columns to use as index 
        metric_cols: List of metric column names to pivot
    
    Returns:
        dictionary of dfs 
    """
    pivot_dfs = {}
    
    for census_variable in variable_names:
        pivot_df = df.pivot(
            index= index_cols,
            columns='year',
            values=census_variable
        ).reset_index()
        pivot_dfs[census_variable] = pivot_df
    
    return pivot_dfs



def print_rankings(df, variable_names, name_col='name', ascending=False):
    """
    Print sorted rankings for multiple metrics and return separate DataFrames.
    
    Args:
        df: DataFrame with metrics
        variable_names: List of metric column names to rank
        name_col: Name of the column with area names
        ascending: Sort order (False = highest first)
    
    Returns:
        Dictionary of DataFrames, one for each metric
    """
    ranked_dfs = {}
    
    for census_variable in variable_names:
        if census_variable in df.columns:
            ranked = df.sort_values(
                by=census_variable,
                ascending=ascending,
                na_position='last'
            ).reset_index(drop=True)[[name_col, census_variable]]
            
            ranked_dfs[census_variable] = ranked
            
            # Print with all rows visible
            with pd.option_context('display.max_rows', None):
                print(ranked)
    
    return ranked_dfs


