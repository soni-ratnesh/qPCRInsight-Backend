
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import f_oneway, ttest_ind, ttest_rel, mannwhitneyu, kruskal, friedmanchisquare


def run_stat_tests(
    df: pd.DataFrame,
    groups: Union[str, List[str]],
    values_col: str,
    test_type: str = "auto",
    paired: bool = False,
    alpha: float = 0.05,
    **kwargs: Any
) -> Dict[str, Any]:
    """Wrapper for statistical tests.
    
    Supports t-test, ANOVA, Mann-Whitney U, Kruskal-Wallis, and Friedman tests.
    Auto-detects appropriate test based on data characteristics if test_type="auto".
    
    Args:
        df: Input DataFrame
        groups: Group column name(s). If list, creates interaction groups
        values_col: Values column name
        test_type: Test type ("auto", "ttest", "anova", "mwu", "kruskal", "friedman")
        paired: Whether samples are paired (for t-test and Friedman)
        alpha: Significance level
        **kwargs: Additional test parameters
        
    Returns:
        Dict[str, Any]: Test results including:
            - test_name: Name of test performed
            - statistic: Test statistic value
            - p_value: P-value
            - significant: Whether p < alpha
            - effect_size: Effect size (when applicable)
            - post_hoc: Post-hoc results (for multi-group tests)
            - assumptions: Test assumptions check results
    """
    # Handle group columns
    if isinstance(groups, list):
        # Create interaction group
        df['_group'] = df[groups].apply(lambda x: '_'.join(x.astype(str)), axis=1)
        group_col = '_group'
    else:
        group_col = groups
    
    # Remove missing values
    clean_df = df[[group_col, values_col]].dropna()
    
    # Get unique groups and their data
    unique_groups = clean_df[group_col].unique()
    group_data = [clean_df[clean_df[group_col] == g][values_col].values for g in unique_groups]
    
    # Auto-detect test type if requested
    if test_type == "auto":
        test_type = _auto_select_test(group_data, paired)
    
    # Run appropriate test
    if test_type == "ttest":
        if len(unique_groups) != 2:
            raise ValueError("T-test requires exactly 2 groups")
        results = _run_ttest(group_data[0], group_data[1], paired, **kwargs)
        
    elif test_type == "anova":
        if len(unique_groups) < 2:
            raise ValueError("ANOVA requires at least 2 groups")
        results = _run_anova(group_data, **kwargs)
        
    elif test_type == "mwu":
        if len(unique_groups) != 2:
            raise ValueError("Mann-Whitney U test requires exactly 2 groups")
        results = _run_mannwhitneyu(group_data[0], group_data[1], **kwargs)
        
    elif test_type == "kruskal":
        if len(unique_groups) < 2:
            raise ValueError("Kruskal-Wallis test requires at least 2 groups")
        results = _run_kruskal(group_data, **kwargs)
        
    elif test_type == "friedman":
        if not paired:
            raise ValueError("Friedman test requires paired data")
        results = _run_friedman(clean_df, group_col, values_col, **kwargs)
        
    else:
        raise ValueError(f"Unknown test type: {test_type}")
    
    # Add common information
    results['groups'] = list(unique_groups)
    results['n_groups'] = len(unique_groups)
    results['alpha'] = alpha
    results['significant'] = results['p_value'] < alpha
    results['sample_sizes'] = {g: len(d) for g, d in zip(unique_groups, group_data)}
    
    # Check assumptions
    results['assumptions'] = _check_assumptions(group_data, test_type)
    
    return results


def _auto_select_test(
    group_data: List[np.ndarray], 
    paired: bool = False
) -> str:
    """Auto-select appropriate statistical test based on data.
    
    Args:
        group_data: List of arrays, one per group
        paired: Whether data is paired
        
    Returns:
        str: Recommended test type
    """
    n_groups = len(group_data)
    
    # Check normality for each group
    normality_results = []
    for data in group_data:
        if len(data) >= 3:  # Need at least 3 samples for normality test
            _, p_norm = stats.shapiro(data) if len(data) <= 5000 else stats.normaltest(data)
            normality_results.append(p_norm > 0.05)
        else:
            normality_results.append(False)
    
    all_normal = all(normality_results) if normality_results else False
    
    # Select test
    if n_groups == 2:
        if all_normal:
            return "ttest"
        else:
            return "mwu"
    else:  # More than 2 groups
        if paired:
            return "friedman"
        elif all_normal:
            return "anova"
        else:
            return "kruskal"


def _run_ttest(
    group1: np.ndarray,
    group2: np.ndarray,
    paired: bool = False,
    equal_var: Optional[bool] = None
) -> Dict[str, Any]:
    """Run t-test."""
    if equal_var is None and not paired:
        # Test for equal variances
        _, p_var = stats.levene(group1, group2)
        equal_var = p_var > 0.05
    
    if paired:
        if len(group1) != len(group2):
            raise ValueError("Paired t-test requires equal sample sizes")
        statistic, p_value = ttest_rel(group1, group2)
        test_name = "Paired t-test"
    else:
        statistic, p_value = ttest_ind(group1, group2, equal_var=equal_var)
        test_name = f"Independent t-test ({'equal' if equal_var else 'unequal'} variance)"
    
    # Calculate effect size (Cohen's d)
    mean_diff = np.mean(group1) - np.mean(group2)
    pooled_std = np.sqrt((np.var(group1, ddof=1) + np.var(group2, ddof=1)) / 2)
    cohens_d = mean_diff / pooled_std if pooled_std > 0 else 0
    
    return {
        'test_name': test_name,
        'statistic': float(statistic),
        'p_value': float(p_value),
        'effect_size': float(cohens_d),
        'effect_size_interpretation': _interpret_cohens_d(cohens_d),
        'mean_difference': float(mean_diff),
        'equal_variance_assumed': equal_var
    }


def _run_anova(group_data: List[np.ndarray]) -> Dict[str, Any]:
    """Run one-way ANOVA."""
    statistic, p_value = f_oneway(*group_data)
    
    # Calculate eta squared (effect size)
    all_data = np.concatenate(group_data)
    grand_mean = np.mean(all_data)
    ss_between = sum(len(g) * (np.mean(g) - grand_mean)**2 for g in group_data)
    ss_total = np.sum((all_data - grand_mean)**2)
    eta_squared = ss_between / ss_total if ss_total > 0 else 0
    
    return {
        'test_name': 'One-way ANOVA',
        'statistic': float(statistic),
        'p_value': float(p_value),
        'effect_size': float(eta_squared),
        'effect_size_interpretation': _interpret_eta_squared(eta_squared),
        'df_between': len(group_data) - 1,
        'df_within': sum(len(g) - 1 for g in group_data)
    }


def _run_mannwhitneyu(
    group1: np.ndarray,
    group2: np.ndarray,
    alternative: str = 'two-sided'
) -> Dict[str, Any]:
    """Run Mann-Whitney U test."""
    statistic, p_value = mannwhitneyu(group1, group2, alternative=alternative)
    
    # Calculate effect size (rank biserial correlation)
    n1, n2 = len(group1), len(group2)
    r = 1 - (2 * statistic) / (n1 * n2)
    
    return {
        'test_name': 'Mann-Whitney U test',
        'statistic': float(statistic),
        'p_value': float(p_value),
        'effect_size': float(r),
        'effect_size_interpretation': _interpret_rank_correlation(r),
        'alternative': alternative
    }


def _run_kruskal(group_data: List[np.ndarray]) -> Dict[str, Any]:
    """Run Kruskal-Wallis test."""
    statistic, p_value = kruskal(*group_data)
    
    # Calculate epsilon squared (effect size)
    n = sum(len(g) for g in group_data)
    epsilon_squared = (statistic - len(group_data) + 1) / (n - len(group_data))
    
    return {
        'test_name': 'Kruskal-Wallis test',
        'statistic': float(statistic),
        'p_value': float(p_value),
        'effect_size': float(epsilon_squared),
        'effect_size_interpretation': _interpret_epsilon_squared(epsilon_squared),
        'df': len(group_data) - 1
    }


def _run_friedman(
    df: pd.DataFrame,
    group_col: str,
    values_col: str
) -> Dict[str, Any]:
    """Run Friedman test for repeated measures."""
    # Reshape data for Friedman test
    pivot = df.pivot_table(values=values_col, columns=group_col, aggfunc='first')
    
    # Remove rows with any missing values
    pivot = pivot.dropna()
    
    if pivot.shape[0] < 2:
        raise ValueError("Friedman test requires at least 2 blocks (subjects)")
    
    # Run test
    statistic, p_value = friedmanchisquare(*[pivot[col].values for col in pivot.columns])
    
    # Calculate Kendall's W (effect size)
    k = pivot.shape[1]  # number of conditions
    n = pivot.shape[0]  # number of subjects
    w = statistic / (n * (k - 1))
    
    return {
        'test_name': 'Friedman test',
        'statistic': float(statistic),
        'p_value': float(p_value),
        'effect_size': float(w),
        'effect_size_interpretation': _interpret_kendalls_w(w),
        'n_blocks': n,
        'n_treatments': k
    }


def _check_assumptions(
    group_data: List[np.ndarray],
    test_type: str
) -> Dict[str, Any]:
    """Check statistical test assumptions."""
    assumptions = {}
    
    # Normality check (for parametric tests)
    if test_type in ['ttest', 'anova']:
        normality_results = []
        for i, data in enumerate(group_data):
            if len(data) >= 3:
                stat, p = stats.shapiro(data) if len(data) <= 5000 else stats.normaltest(data)
                normality_results.append({
                    'group': i,
                    'statistic': float(stat),
                    'p_value': float(p),
                    'normal': p > 0.05
                })
        assumptions['normality'] = normality_results
    
    # Homogeneity of variance (for t-test and ANOVA)
    if test_type in ['ttest', 'anova'] and len(group_data) >= 2:
        stat, p = stats.levene(*group_data)
        assumptions['equal_variance'] = {
            'statistic': float(stat),
            'p_value': float(p),
            'equal': p > 0.05
        }
    
    return assumptions


def _interpret_cohens_d(d: float) -> str:
    """Interpret Cohen's d effect size."""
    d = abs(d)
    if d < 0.2:
        return "negligible"
    elif d < 0.5:
        return "small"
    elif d < 0.8:
        return "medium"
    else:
        return "large"


def _interpret_eta_squared(eta2: float) -> str:
    """Interpret eta squared effect size."""
    if eta2 < 0.01:
        return "negligible"
    elif eta2 < 0.06:
        return "small"
    elif eta2 < 0.14:
        return "medium"
    else:
        return "large"


def _interpret_rank_correlation(r: float) -> str:
    """Interpret rank correlation effect size."""
    r = abs(r)
    if r < 0.1:
        return "negligible"
    elif r < 0.3:
        return "small"
    elif r < 0.5:
        return "medium"
    else:
        return "large"


def _interpret_epsilon_squared(eps2: float) -> str:
    """Interpret epsilon squared effect size."""
    return _interpret_eta_squared(eps2)  # Same interpretation


def _interpret_kendalls_w(w: float) -> str:
    """Interpret Kendall's W effect size."""
    if w < 0.1:
        return "negligible"
    elif w < 0.3:
        return "small"
    elif w < 0.5:
        return "medium"
    else:
        return "large"