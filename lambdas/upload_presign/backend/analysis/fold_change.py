
from typing import Optional, List

import numpy as np
import pandas as pd


def compute_fold_change(
    df: pd.DataFrame,
    delta_delta_ct_col: str = "delta_delta_ct",
    log2_transform: bool = False,
    add_confidence_interval: bool = True,
    ct_std_col: Optional[str] = "ct_std"
) -> pd.DataFrame:
    """Calculate fold change from ΔΔCt values.
    
    Computes 2**(-ΔΔCt) with optional log2 transformation and confidence intervals.
    
    Args:
        df: Input DataFrame with ΔΔCt values
        delta_delta_ct_col: Column name containing ΔΔCt values
        log2_transform: Whether to apply log2 transformation to fold change
        add_confidence_interval: Whether to calculate 95% CI
        ct_std_col: Column name containing CT standard deviations (for CI)
        
    Returns:
        pd.DataFrame: DataFrame with fold change values and optionally CI
        
    Raises:
        ValueError: If required columns are missing
    """
    if delta_delta_ct_col not in df.columns:
        raise ValueError(f"Column '{delta_delta_ct_col}' not found in DataFrame")
    
    df = df.copy()
    
    # Calculate fold change: 2^(-ΔΔCt)
    df['fold_change'] = 2 ** (-df[delta_delta_ct_col])
    
    # Apply log2 transformation if requested
    if log2_transform:
        # Add small epsilon to avoid log(0)
        epsilon = 1e-10
        df['log2_fold_change'] = np.log2(df['fold_change'] + epsilon)
    
    # Calculate confidence intervals if requested and std available
    if add_confidence_interval and ct_std_col in df.columns:
        df = _add_confidence_intervals(df, delta_delta_ct_col, ct_std_col)
    
    # Add interpretation
    df['regulation'] = df.apply(_classify_regulation, axis=1)
    
    return df


def _add_confidence_intervals(
    df: pd.DataFrame,
    delta_delta_ct_col: str,
    ct_std_col: str,
    confidence_level: float = 0.95
) -> pd.DataFrame:
    """Add confidence intervals for fold change.
    
    Uses error propagation to calculate CI based on CT standard deviations.
    
    Args:
        df: Input DataFrame
        delta_delta_ct_col: Column with ΔΔCt values
        ct_std_col: Column with CT standard deviations
        confidence_level: Confidence level (default 0.95 for 95% CI)
        
    Returns:
        pd.DataFrame: DataFrame with CI columns added
    """
    # Z-score for confidence level (1.96 for 95% CI)
    from scipy import stats
    z_score = stats.norm.ppf((1 + confidence_level) / 2)
    
    # Error propagation for ΔΔCt (assuming independent errors)
    # SE(ΔΔCt) ≈ SE(ΔCt) * √2 (simplified assumption)
    se_delta_delta_ct = df[ct_std_col] * np.sqrt(2)
    
    # Calculate CI bounds for ΔΔCt
    ci_lower_ddct = df[delta_delta_ct_col] - z_score * se_delta_delta_ct
    ci_upper_ddct = df[delta_delta_ct_col] + z_score * se_delta_delta_ct
    
    # Convert to fold change
    # Note: Because of negative exponent, bounds are reversed
    df['fold_change_ci_lower'] = 2 ** (-ci_upper_ddct)
    df['fold_change_ci_upper'] = 2 ** (-ci_lower_ddct)
    
    # Calculate CI width
    df['fold_change_ci_width'] = df['fold_change_ci_upper'] - df['fold_change_ci_lower']
    
    return df


def _classify_regulation(row: pd.Series, threshold: float = 2.0) -> str:
    """Classify gene regulation based on fold change.
    
    Args:
        row: DataFrame row with fold_change column
        threshold: Fold change threshold for up/down regulation
        
    Returns:
        str: Regulation classification
    """
    if pd.isna(row['fold_change']):
        return 'Undetermined'
    elif row['fold_change'] >= threshold:
        return 'Upregulated'
    elif row['fold_change'] <= 1/threshold:
        return 'Downregulated'
    else:
        return 'No change'


def summarize_fold_changes(
    df: pd.DataFrame,
    group_by: Optional[List[str]] = None,
    fold_change_col: str = 'fold_change'
) -> pd.DataFrame:
    """Summarize fold changes by groups.
    
    Args:
        df: Input DataFrame with fold changes
        group_by: Columns to group by (default: ['Target Name'])
        fold_change_col: Column containing fold change values
        
    Returns:
        pd.DataFrame: Summary statistics by group
    """
    if group_by is None:
        group_by = ['Target Name']
    
    summary = df.groupby(group_by).agg({
        fold_change_col: ['mean', 'std', 'min', 'max', 'count']
    }).round(4)
    
    summary.columns = ['mean_fc', 'std_fc', 'min_fc', 'max_fc', 'n_samples']
    
    # Add geometric mean (more appropriate for fold changes)
    geo_means = df.groupby(group_by)[fold_change_col].apply(
        lambda x: np.exp(np.log(x[x > 0]).mean()) if len(x[x > 0]) > 0 else np.nan
    )
    summary['geometric_mean_fc'] = geo_means.round(4)
    
    return summary.reset_index()