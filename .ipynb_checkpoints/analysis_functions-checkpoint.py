from census import Census
import pandas as pd
from us import states


def fetch_census_data(census_obj, geography, years, variables, rename_columns=None):
    """
    Fetch census data for metropolitan statistical areas or counties for multiple years.
    Returns a DataFrame with renamed columns. Uses ACS 1-year estimates.
    
    Arguments:
        census_obj: census API object
        geography: 'msa' or 'county'
        years: list of years to fetch data for
        variables: census variable codes
        rename_columns: optional dict to rename columns. If None, uses default mapping.
    
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
    
    for year in years:
        try:
            if geography == 'msa':
                # if geography is MSA, fetch census data for all MSAs
                data = census_obj.acs1.get(
                    variables,
                    geo={'for': 'metropolitan statistical area/micropolitan statistical area:*'},
                    year=year
                )
            elif geography == 'county':
                # if geography is county, fetch census data for all counties in Texas
                data = census_obj.acs1.state_county(
                    variables,
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
        
        except Exception as e:
            print(f"Error for year {year}: {e}")
    
    # convert to df
    df = pd.DataFrame(all_data)
    
    # use default
    if rename_columns is None:
        rename_columns = msa_mapping if geography == 'msa' else county_mapping
    
    # rename columns so they are readable
    df = df.rename(columns=rename_columns)
    
    return df


def fetch_census_data_acs5(census_obj, geography, years, variables, rename_columns=None):
    """
    Fetch census data for metropolitan statistical areas or counties for multiple years.
    Returns a DataFrame with renamed columns. Uses ACS 5-year estimates.
    
    Arguments:
        census_obj: census API object
        geography: 'msa' or 'county'
        years: list of years to fetch data for
        variables: census variable codes
        rename_columns: optional dict to rename columns. If None, uses default mapping.
    
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
    
    for year in years:
        try:
            if geography == 'msa':
                # if geography is MSA, fetch census data for all MSAs using ACS 5-year
                data = census_obj.acs5.get(
                    variables,
                    geo={'for': 'metropolitan statistical area/micropolitan statistical area:*'},
                    year=year
                )
            elif geography == 'county':
                # if geography is county, fetch census data for all counties in Texas using ACS 5-year
                data = census_obj.acs5.state_county(
                    variables,
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
        
        except Exception as e:
            print(f"Error for year {year}: {e}")
    
    # convert to df
    df = pd.DataFrame(all_data)
    
    # use default
    if rename_columns is None:
        rename_columns = msa_mapping if geography == 'msa' else county_mapping
    
    # rename columns so they are readable
    df = df.rename(columns=rename_columns)
    
    return df


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
    Print sorted rankings for multiple metrics.
    
    Args:
        df: DataFrame with metrics
        metric_cols: List of metric column names to rank
        name_col: Name of the column with area names
        ascending: Sort order (False = highest first)
    """
    for census_variable in variable_names:
        if census_variable in df.columns:
            ranked = df.sort_values(
                by=census_variable,
                ascending=ascending,
                na_position='last'
            ).reset_index(drop=True)[[name_col, census_variable]]
            print(ranked)


