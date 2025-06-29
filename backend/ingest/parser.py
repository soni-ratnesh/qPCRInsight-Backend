from typing import Any, Dict, Tuple

import pandas as pd
import numpy as np


def parse_applied_biosystems_xlsx(file_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Parse Applied Biosystems qPCR Excel file.
    
    Extracts metadata and well data from the Results sheet of an Applied Biosystems
    qPCR Excel file. Handles the specific format where metadata appears at the top
    followed by well data.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        Tuple[pd.DataFrame, Dict[str, Any]]: 
            - DataFrame with well data (columns: Well, Sample Name, Target Name, CT, etc.)
            - Dictionary containing file metadata
            
    Raises:
        ValueError: If file format is invalid or required columns are missing
    """
    try:
        # Read Excel file without headers to preserve structure
        df = pd.read_excel(file_path, 'Results', header=None).dropna(axis=1, how='all')
    except Exception as e:
        raise ValueError(f"Failed to read Excel file: {str(e)}")
    
    metadata = {}
    well_data_start_index = 0
    
    # Extract metadata from top rows
    for i, row in df.iterrows():
        if pd.isna(row[0]):
            well_data_start_index = i + 1
            break
        metadata[str(row[0])] = row[1]
    
    # Extract well data
    if well_data_start_index >= len(df) - 1:
        raise ValueError("No well data found in file")
    
    # Set column headers from the row after metadata
    well_data = df.iloc[well_data_start_index + 1:]
    well_data.columns = list(df.iloc[well_data_start_index])
    
    # Clean up data
    well_data = well_data.dropna(how='all').reset_index(drop=True)
    
    # Validate required columns
    required_columns = ['Sample Name', 'Target Name', 'CT']
    missing_columns = [col for col in required_columns if col not in well_data.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Convert CT values to numeric, handling "Undetermined" values
    well_data['CT'] = pd.to_numeric(well_data['CT'], errors='coerce')
    
    # Add metadata to the dataframe attributes for reference
    well_data.attrs['metadata'] = metadata
    
    return well_data, metadata


def validate_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data quality and flag issues.
    
    Args:
        df: DataFrame with well data
        
    Returns:
        pd.DataFrame: DataFrame with additional quality flags
    """
    df = df.copy()
    
    # Flag undetermined CT values
    df['ct_undetermined'] = df['CT'].isna()
    
    # Flag extreme CT values (typically >35 indicates very low expression)
    df['ct_high'] = df['CT'] > 35
    
    # Count replicates per sample/target combination
    replicate_counts = df.groupby(['Sample Name', 'Target Name']).size()
    df = df.merge(
        replicate_counts.rename('replicate_count').reset_index(),
        on=['Sample Name', 'Target Name'],
        how='left'
    )
    
    return df