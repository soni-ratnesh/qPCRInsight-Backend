
from typing import Any, Dict, List, Optional
import pandas as pd


def combine_notebook(
    df_mean: pd.DataFrame, 
    df_std: pd.DataFrame,
    mean_suffix: str = "Mean",
    std_suffix: str = "SD"
) -> pd.DataFrame:
    """Combine mean and standard deviation dataframes into notebook format.
    
    Creates a multi-level column structure where each original column
    is split into Mean and SD sub-columns, interleaved for easy reading.
    
    Args:
        df_mean: DataFrame containing mean values
        df_std: DataFrame containing standard deviation values
        mean_suffix: Suffix for mean columns
        std_suffix: Suffix for SD columns
        
    Returns:
        pd.DataFrame: Combined DataFrame with multi-level columns
    """
    # Create copies to avoid modifying originals
    df_mean = df_mean.copy()
    df_std = df_std.copy()
    
    # Ensure both dataframes have the same structure
    if not df_mean.index.equals(df_std.index):
        raise ValueError("Mean and SD dataframes must have the same index")
    if not df_mean.columns.equals(df_std.columns):
        raise ValueError("Mean and SD dataframes must have the same columns")
    
    # Create multi-level columns
    df_mean.columns = pd.MultiIndex.from_product([df_mean.columns, [mean_suffix]])
    df_std.columns = pd.MultiIndex.from_product([df_std.columns, [std_suffix]])
    
    # Concatenate and sort to interleave Mean/SD columns
    combined = pd.concat([df_mean, df_std], axis=1).sort_index(axis=1, level=0)
    
    return combined


def create_summary_statistics(
    df: pd.DataFrame,
    value_columns: List[str],
    group_by: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """Create summary statistics for report.
    
    Args:
        df: Input DataFrame
        value_columns: Columns to calculate statistics for
        group_by: Optional grouping columns
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of summary tables
    """
    summaries = {}
    
    # Overall statistics
    overall_stats = df[value_columns].describe()
    summaries['overall_statistics'] = overall_stats
    
    # Group statistics if specified
    if group_by:
        for col in group_by:
            if col in df.columns:
                group_stats = df.groupby(col)[value_columns].describe()
                summaries[f'statistics_by_{col}'] = group_stats
    
    # Missing data summary
    missing_summary = pd.DataFrame({
        'column': value_columns,
        'missing_count': [df[col].isna().sum() for col in value_columns],
        'missing_percentage': [
            df[col].isna().sum() / len(df) * 100 for col in value_columns
        ]
    })
    summaries['missing_data_summary'] = missing_summary
    
    return summaries


def format_excel_sheets(
    writer: pd.ExcelWriter,
    sheet_formats: Optional[Dict[str, Dict[str, Any]]] = None
) -> None:
    """Apply formatting to Excel sheets.
    
    Args:
        writer: Excel writer object
        sheet_formats: Dictionary of sheet names to format specifications
    """
    workbook = writer.book
    
    # Default formats
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#D7E4BD',
        'border': 1
    })
    
    number_format = workbook.add_format({
        'num_format': '0.0000'
    })
    
    percent_format = workbook.add_format({
        'num_format': '0.00%'
    })
    
    # Apply default formatting to all sheets
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        
        # Set column widths
        worksheet.set_column('A:A', 20)  # First column wider for labels
        worksheet.set_column('B:Z', 12)  # Other columns standard width
        
        # Apply specific formats if provided
        if sheet_formats and sheet_name in sheet_formats:
            sheet_fmt = sheet_formats[sheet_name]
            if 'column_widths' in sheet_fmt:
                for col, width in sheet_fmt['column_widths'].items():
                    worksheet.set_column(f'{col}:{col}', width)