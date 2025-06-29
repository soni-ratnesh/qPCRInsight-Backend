
from typing import List, Dict, Any, Tuple
from typing_extensions import Literal
import itertools

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats import multitest
from statsmodels.stats.multicomp import pairwise_tukeyhsd


def adjust_pvalues(
    pvalues: List[float],
    method: Literal["fdr_bh", "holm", "bonferroni"] = "fdr_bh",
    alpha: float = 0.05
) -> np.ndarray:
    """Adjust p-values for multiple comparisons.
    
    Args:
        pvalues: List of p-values
        method: Adjustment method
            - "fdr_bh": Benjamini-Hochberg (False Discovery Rate)
            - "holm": Holm-Bonferroni method
            - "bonferroni": Bonferroni correction
        alpha: Significance level
        
    Returns:
        np.ndarray: Adjusted p-values
    """
    if not pvalues:
        return np.array([])
    
    # Convert to numpy array
    pvalues = np.array(pvalues)
    
    # Handle NaN values
    nan_mask = np.isnan(pvalues)
    if nan_mask.all():
        return pvalues
    
    # Adjust p-values
    adjusted = np.full_like(pvalues, np.nan)
    if not nan_mask.all():
        _, adjusted[~nan_mask], _, _ = multitest.multipletests(
            pvalues[~nan_mask], 
            alpha=alpha, 
            method=method
        )
    
    return adjusted


def run_pairwise_tests(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    test_type: str = "tukey",
    p_adjust: str = "fdr_bh",
    parametric: bool = True
) -> pd.DataFrame:
    """Run pairwise post-hoc tests.
    
    Args:
        df: Input DataFrame
        group_col: Column containing group labels
        value_col: Column containing values to test
        test_type: Type of pairwise test
            - "tukey": Tukey HSD (parametric)
            - "dunn": Dunn's test (non-parametric)
            - "pairwise_t": Pairwise t-tests
            - "pairwise_mwu": Pairwise Mann-Whitney U tests
        p_adjust: P-value adjustment method
        parametric: Whether to use parametric tests
        
    Returns:
        pd.DataFrame: Results with columns:
            - group1, group2: Groups being compared
            - statistic: Test statistic
            - p_value: Raw p-value
            - p_adjusted: Adjusted p-value
            - significant: Whether comparison is significant
            - mean_diff: Mean difference (for parametric tests)
    """
    # Clean data
    clean_df = df[[group_col, value_col]].dropna()
    groups = clean_df[group_col].unique()
    
    if len(groups) < 2:
        raise ValueError("Need at least 2 groups for pairwise comparisons")
    
    if test_type == "tukey" and parametric:
        return _tukey_hsd(clean_df, group_col, value_col, p_adjust)
    elif test_type == "dunn" or not parametric:
        return _dunn_test(clean_df, group_col, value_col, p_adjust)
    elif test_type == "pairwise_t":
        return _pairwise_ttests(clean_df, group_col, value_col, p_adjust)
    elif test_type == "pairwise_mwu":
        return _pairwise_mwu(clean_df, group_col, value_col, p_adjust)
    else:
        raise ValueError(f"Unknown test type: {test_type}")


def _tukey_hsd(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    p_adjust: str
) -> pd.DataFrame:
    """Perform Tukey's HSD test."""
    # Run Tukey HSD
    tukey = pairwise_tukeyhsd(
        endog=df[value_col],
        groups=df[group_col],
        alpha=0.05
    )
    
    # Extract results
    results = []
    for row in tukey.summary().data[1:]:  # Skip header
        results.append({
            'group1': row[0],
            'group2': row[1],
            'mean_diff': float(row[2]),
            'p_value': float(row[5]),
            'lower_ci': float(row[4]),
            'upper_ci': float(row[6])
        })
    
    results_df = pd.DataFrame(results)
    
    # Tukey already adjusts for multiple comparisons, but add adjusted column for consistency
    results_df['p_adjusted'] = results_df['p_value']
    results_df['significant'] = results_df['p_adjusted'] < 0.05
    
    return results_df


def _dunn_test(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    p_adjust: str
) -> pd.DataFrame:
    """Perform Dunn's test (non-parametric post-hoc)."""
    groups = df[group_col].unique()
    n_groups = len(groups)
    
    # Rank all data
    df['rank'] = df[value_col].rank()
    
    # Calculate mean ranks for each group
    mean_ranks = df.groupby(group_col)['rank'].agg(['mean', 'count'])
    
    # Total number of observations
    N = len(df)
    
    # Calculate average rank
    avg_rank = (N + 1) / 2
    
    # Calculate z-scores for all pairwise comparisons
    results = []
    comparisons = list(itertools.combinations(groups, 2))
    
    for g1, g2 in comparisons:
        n1 = mean_ranks.loc[g1, 'count']
        n2 = mean_ranks.loc[g2, 'count']
        r1 = mean_ranks.loc[g1, 'mean']
        r2 = mean_ranks.loc[g2, 'mean']
        
        # Standard error
        se = np.sqrt((N * (N + 1) / 12) * (1/n1 + 1/n2))
        
        # Z-score
        z = (r1 - r2) / se if se > 0 else 0
        
        # Two-tailed p-value
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        results.append({
            'group1': g1,
            'group2': g2,
            'statistic': z,
            'p_value': p_value,
            'mean_rank_diff': r1 - r2
        })
    
    results_df = pd.DataFrame(results)
    
    # Adjust p-values
    results_df['p_adjusted'] = adjust_pvalues(
        results_df['p_value'].tolist(),
        method=p_adjust
    )
    results_df['significant'] = results_df['p_adjusted'] < 0.05
    
    return results_df


def _pairwise_ttests(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    p_adjust: str
) -> pd.DataFrame:
    """Perform pairwise t-tests."""
    groups = df[group_col].unique()
    comparisons = list(itertools.combinations(groups, 2))
    
    results = []
    for g1, g2 in comparisons:
        data1 = df[df[group_col] == g1][value_col].values
        data2 = df[df[group_col] == g2][value_col].values
        
        # Run t-test
        statistic, p_value = stats.ttest_ind(data1, data2)
        
        # Calculate effect size (Cohen's d)
        mean_diff = np.mean(data1) - np.mean(data2)
        pooled_std = np.sqrt((np.var(data1, ddof=1) + np.var(data2, ddof=1)) / 2)
        cohens_d = mean_diff / pooled_std if pooled_std > 0 else 0
        
        results.append({
            'group1': g1,
            'group2': g2,
            'statistic': statistic,
            'p_value': p_value,
            'mean_diff': mean_diff,
            'cohens_d': cohens_d
        })
    
    results_df = pd.DataFrame(results)
    
    # Adjust p-values
    results_df['p_adjusted'] = adjust_pvalues(
        results_df['p_value'].tolist(),
        method=p_adjust
    )
    results_df['significant'] = results_df['p_adjusted'] < 0.05
    
    return results_df


def _pairwise_mwu(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    p_adjust: str
) -> pd.DataFrame:
    """Perform pairwise Mann-Whitney U tests."""
    groups = df[group_col].unique()
    comparisons = list(itertools.combinations(groups, 2))
    
    results = []
    for g1, g2 in comparisons:
        data1 = df[df[group_col] == g1][value_col].values
        data2 = df[df[group_col] == g2][value_col].values
        
        # Run Mann-Whitney U test
        statistic, p_value = stats.mannwhitneyu(data1, data2, alternative='two-sided')
        
        # Calculate effect size (rank biserial correlation)
        n1, n2 = len(data1), len(data2)
        r = 1 - (2 * statistic) / (n1 * n2)
        
        results.append({
            'group1': g1,
            'group2': g2,
            'statistic': statistic,
            'p_value': p_value,
            'rank_biserial': r,
            'median_diff': np.median(data1) - np.median(data2)
        })
    
    results_df = pd.DataFrame(results)
    
    # Adjust p-values
    results_df['p_adjusted'] = adjust_pvalues(
        results_df['p_value'].tolist(),
        method=p_adjust
    )
    results_df['significant'] = results_df['p_adjusted'] < 0.05
    
    return results_df


def create_comparison_matrix(
    pairwise_results: pd.DataFrame,
    value_col: str = 'p_adjusted',
    groups: List[str] = None
) -> pd.DataFrame:
    """Create a comparison matrix from pairwise results.
    
    Args:
        pairwise_results: DataFrame with pairwise comparison results
        value_col: Column to use for matrix values
        groups: Optional list of groups to include (in order)
        
    Returns:
        pd.DataFrame: Square matrix with comparisons
    """
    # Get unique groups if not provided
    if groups is None:
        groups = sorted(set(
            list(pairwise_results['group1'].unique()) + 
            list(pairwise_results['group2'].unique())
        ))
    
    # Initialize matrix
    matrix = pd.DataFrame(
        index=groups,
        columns=groups,
        dtype=float
    )
    
    # Fill diagonal with 1.0 (comparing group to itself)
    np.fill_diagonal(matrix.values, 1.0)
    
    # Fill matrix with values
    for _, row in pairwise_results.iterrows():
        g1, g2 = row['group1'], row['group2']
        value = row[value_col]
        
        if g1 in groups and g2 in groups:
            matrix.loc[g1, g2] = value
            matrix.loc[g2, g1] = value
    
    return matrix


def summarize_posthoc_results(
    pairwise_results: pd.DataFrame,
    target_col: str = None
) -> Dict[str, Any]:
    """Summarize post-hoc results.
    
    Args:
        pairwise_results: DataFrame with pairwise comparison results
        target_col: Optional column to group results by (e.g., 'Target Name')
        
    Returns:
        Dict[str, Any]: Summary including:
            - n_comparisons: Total number of comparisons
            - n_significant: Number of significant comparisons
            - significance_rate: Proportion of significant comparisons
            - summary_table: Summary by group
    """
    summary = {
        'n_comparisons': len(pairwise_results),
        'n_significant': pairwise_results['significant'].sum(),
        'significance_rate': pairwise_results['significant'].mean()
    }
    
    # Create summary table
    if target_col and target_col in pairwise_results.columns:
        summary_table = pairwise_results.groupby(target_col).agg({
            'significant': ['sum', 'count', 'mean'],
            'p_adjusted': 'min'
        })
        summary_table.columns = ['n_significant', 'n_comparisons', 
                                'significance_rate', 'min_p_adjusted']
    else:
        # Summarize by group
        all_groups = set(pairwise_results['group1']) | set(pairwise_results['group2'])
        group_summary = []
        
        for group in all_groups:
            mask = (pairwise_results['group1'] == group) | (pairwise_results['group2'] == group)
            group_data = pairwise_results[mask]
            
            group_summary.append({
                'group': group,
                'n_comparisons': len(group_data),
                'n_significant': group_data['significant'].sum(),
                'min_p_adjusted': group_data['p_adjusted'].min()
            })
        
        summary_table = pd.DataFrame(group_summary)
    
    summary['summary_table'] = summary_table
    
    return summary