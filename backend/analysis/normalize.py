# backend/analysis/normalize.py
from __future__ import annotations

from typing import Optional

import pandas as pd


def compute_delta_ct(
    df: pd.DataFrame,
    reference_gene: str,
    target_genes: list[str]
) -> pd.DataFrame:
    """Compute ΔCt values.
    
    Args:
        df: Input DataFrame with Ct values
        reference_gene: Reference gene name
        target_genes: List of target gene names
        
    Returns:
        pd.DataFrame: DataFrame with ΔCt values
    """
    # Already implemented - copy in unchanged
    pass  # TODO: implement


def compute_delta_delta_ct(
    df: pd.DataFrame,
    control_condition: str,
    experimental_conditions: list[str]
) -> pd.DataFrame:
    """Compute ΔΔCt values.
    
    Args:
        df: Input DataFrame with ΔCt values
        control_condition: Control condition name
        experimental_conditions: List of experimental condition names
        
    Returns:
        pd.DataFrame: DataFrame with ΔΔCt values
    """
    # Already implemented - copy in unchanged
    pass  # TODO: implement