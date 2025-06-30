
from typing import Optional, List, Union

import pandas as pd
import numpy as np


def compute_delta_ct(
    df: pd.DataFrame,
    reference_gene: str,
    target_genes: Optional[List[str]] = None,
    ct_column: str = 'ct_mean',
    sample_column: str = 'Sample Name',
    target_column: str = 'Target Name'
) -> pd.DataFrame:
    """Compute ΔCt values (target - reference).
    
    Calculates ΔCt for each target gene relative to the reference gene
    within each sample. ΔCt = Ct(target) - Ct(reference)
    
    Args:
        df: Input DataFrame with CT values (should be means from replicates)
        reference_gene: Reference/housekeeping gene name (e.g., 'GAPDH')
        target_genes: List of target gene names. If None, all non-reference genes
        ct_column: Column containing CT values
        sample_column: Column containing sample names
        target_column: Column containing target/gene names
        
    Returns:
        pd.DataFrame: DataFrame with ΔCt values added
        
    Raises:
        ValueError: If reference gene not found or data structure invalid
    """
    # Ensure we have the required columns
    required_cols = [sample_column, target_column, ct_column]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check reference gene exists
    if reference_gene not in df[target_column].unique():
        raise ValueError(f"Reference gene '{reference_gene}' not found in data")
    
    # Pivot to get genes as columns
    pivot_df = df.pivot_table(
        index=sample_column,
        columns=target_column,
        values=ct_column,
        aggfunc='first'
    )
    
    # Get reference gene values
    if reference_gene not in pivot_df.columns:
        raise ValueError(f"Reference gene '{reference_gene}' has no CT values")
    
    reference_values = pivot_df[reference_gene]
    
    # Determine target genes
    if target_genes is None:
        target_genes = [col for col in pivot_df.columns if col != reference_gene]
    else:
        # Validate target genes exist
        missing_targets = [g for g in target_genes if g not in pivot_df.columns]
        if missing_targets:
            raise ValueError(f"Target genes not found: {missing_targets}")
    
    # Calculate ΔCt for each target
    delta_ct_data = {}
    for target in target_genes:
        delta_ct_data[target] = pivot_df[target] - reference_values
    
    # Add reference gene (ΔCt = 0 for reference vs itself)
    delta_ct_data[reference_gene] = pd.Series(0, index=pivot_df.index)
    
    # Create result DataFrame
    delta_ct_df = pd.DataFrame(delta_ct_data)
    
    # Melt back to long format
    result = delta_ct_df.reset_index().melt(
        id_vars=[sample_column],
        var_name=target_column,
        value_name='delta_ct'
    )
    
    # Merge with original data to preserve other columns
    result = df.merge(
        result,
        on=[sample_column, target_column],
        how='left'
    )
    
    # Add reference gene info
    result['reference_gene'] = reference_gene
    
    return result


def compute_delta_delta_ct(
    df: pd.DataFrame,
    control_condition: str,
    experimental_conditions: Optional[List[str]] = None,
    delta_ct_column: str = 'delta_ct',
    sample_column: str = 'Sample Name',
    target_column: str = 'Target Name'
) -> pd.DataFrame:
    """Compute ΔΔCt values (experimental - control).
    
    Calculates ΔΔCt for each experimental condition relative to the control
    condition. ΔΔCt = ΔCt(experimental) - ΔCt(control)
    
    Args:
        df: Input DataFrame with ΔCt values
        control_condition: Control condition sample name
        experimental_conditions: List of experimental condition names. 
                                If None, all non-control samples
        delta_ct_column: Column containing ΔCt values
        sample_column: Column containing sample names
        target_column: Column containing target/gene names
        
    Returns:
        pd.DataFrame: DataFrame with ΔΔCt values added
        
    Raises:
        ValueError: If control condition not found or data structure invalid
    """
    # Validate inputs
    if control_condition not in df[sample_column].unique():
        raise ValueError(f"Control condition '{control_condition}' not found")
    
    # Pivot to get samples as columns
    pivot_df = df.pivot_table(
        index=target_column,
        columns=sample_column,
        values=delta_ct_column,
        aggfunc='first'
    )
    
    # Get control values
    control_values = pivot_df[control_condition]
    
    # Determine experimental conditions
    if experimental_conditions is None:
        experimental_conditions = [
            col for col in pivot_df.columns if col != control_condition
        ]
    
    # Calculate ΔΔCt
    delta_delta_ct_data = {}
    for condition in experimental_conditions:
        delta_delta_ct_data[condition] = pivot_df[condition] - control_values
    
    # Add control (ΔΔCt = 0 for control vs itself)
    delta_delta_ct_data[control_condition] = pd.Series(0, index=pivot_df.index)
    
    # Create result DataFrame
    delta_delta_ct_df = pd.DataFrame(delta_delta_ct_data)
    
    # Melt back to long format
    result = delta_delta_ct_df.reset_index().melt(
        id_vars=[target_column],
        var_name=sample_column,
        value_name='delta_delta_ct'
    )
    
    # Merge with original data
    result = df.merge(
        result,
        on=[sample_column, target_column],
        how='left'
    )
    
    # Add control condition info
    result['control_condition'] = control_condition
    
    return result