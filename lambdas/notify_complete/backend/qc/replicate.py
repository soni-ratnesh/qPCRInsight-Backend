
from typing import Tuple, Optional, List

import pandas as pd
import numpy as np


def filter_replicates(
    df: pd.DataFrame,
    sd_cutoff: float = 0.5,
    min_proportion: float = 0.5,
    ct_column: str = 'CT'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Drop/flag wells by SD and proportion logic.
    
    Filters technical replicates based on standard deviation and minimum
    proportion of valid (non-NA) values. Groups are defined by Sample Name
    and Target Name combinations.
    
    Args:
        df: Input DataFrame with replicate wells
        sd_cutoff: Maximum acceptable standard deviation
        min_proportion: Minimum proportion of valid replicates
        ct_column: Name of the CT value column
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: 
            - Filtered data (replicates meeting criteria)
            - Flagged wells (replicates failing criteria)
    """
    df = df.copy()
    
    # Calculate statistics per replicate group
    grouped = df.groupby(['Sample Name', 'Target Name'])
    
    # Calculate SD and valid proportion for each group
    stats = grouped.agg({
        ct_column: ['std', 'count', lambda x: x.notna().sum()]
    }).round(4)
    stats.columns = ['sd', 'total_count', 'valid_count']
    stats['valid_proportion'] = stats['valid_count'] / stats['total_count']
    
    # Identify groups that meet criteria
    valid_groups = stats[
        (stats['sd'] <= sd_cutoff) & 
        (stats['valid_proportion'] >= min_proportion)
    ].index
    
    # Split data into filtered and flagged
    mask = df.set_index(['Sample Name', 'Target Name']).index.isin(valid_groups)
    filtered_df = df[mask].copy()
    flagged_df = df[~mask].copy()
    
    # Add QC metrics to both dataframes
    if not filtered_df.empty:
        filtered_df = filtered_df.merge(
            stats.reset_index(),
            on=['Sample Name', 'Target Name'],
            how='left'
        )
    
    if not flagged_df.empty:
        flagged_df = flagged_df.merge(
            stats.reset_index(),
            on=['Sample Name', 'Target Name'],
            how='left'
        )
        
        # Add reason for flagging
        flagged_df['qc_fail_reason'] = flagged_df.apply(
            lambda row: _get_fail_reason(row, sd_cutoff, min_proportion),
            axis=1
        )
    
    return filtered_df, flagged_df


def _get_fail_reason(row: pd.Series, sd_cutoff: float, min_proportion: float) -> str:
    """Determine reason for QC failure.
    
    Args:
        row: DataFrame row with QC statistics
        sd_cutoff: Maximum acceptable standard deviation
        min_proportion: Minimum proportion threshold
        
    Returns:
        str: Reason for QC failure
    """
    reasons = []
    
    # Check if required columns exist
    if 'sd' in row and pd.notna(row['sd']) and row['sd'] > sd_cutoff:
        reasons.append(f"SD ({row['sd']:.3f}) > cutoff ({sd_cutoff})")
    
    if 'valid_proportion' in row and pd.notna(row['valid_proportion']) and row['valid_proportion'] < min_proportion:
        reasons.append(
            f"Valid proportion ({row['valid_proportion']:.2f}) < minimum ({min_proportion})"
        )
    
    return "; ".join(reasons) if reasons else "Unknown"


def calculate_replicate_means(
    df: pd.DataFrame,
    ct_column: str = 'CT',
    group_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Calculate mean CT values for technical replicates.
    
    Args:
        df: Input DataFrame with replicate wells
        ct_column: Name of the CT value column
        group_columns: Columns to group by (default: Sample Name, Target Name)
        
    Returns:
        pd.DataFrame: DataFrame with mean CT values and statistics
    """
    if group_columns is None:
        group_columns = ['Sample Name', 'Target Name']
    
    # Group and calculate statistics
    grouped = df.groupby(group_columns)
    
    result = grouped.agg({
        ct_column: ['mean', 'std', 'count', lambda x: x.notna().sum()]
    }).round(4)
    
    result.columns = ['ct_mean', 'ct_std', 'total_replicates', 'valid_replicates']
    result = result.reset_index()
    
    # Add coefficient of variation
    result['ct_cv'] = (result['ct_std'] / result['ct_mean'] * 100).round(2)
    
    return result