
from typing import Any, Dict, List, Optional
import os
from datetime import datetime

import pandas as pd
import xlsxwriter
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

from backend.report.utils import combine_notebook, format_excel_sheets


def build_xlsx_report(
    data: Dict[str, pd.DataFrame],
    metadata: Dict[str, Any],
    output_path: str,
    include_plots: Optional[List[str]] = None
) -> str:
    """Build multi-sheet Excel workbook report.
    
    Creates a comprehensive Excel report with multiple sheets containing:
    - Raw data
    - Mean values with SD
    - Delta CT calculations
    - Delta Delta CT calculations
    - Fold changes
    - Statistical results
    - Metadata
    
    Args:
        data: Dictionary of sheet names to DataFrames
        metadata: Report metadata (experiment info, parameters, etc.)
        output_path: Output file path
        include_plots: Optional list of plot image paths to include
        
    Returns:
        str: Path to generated Excel file
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Create Excel writer
    writer = pd.ExcelWriter(output_path, engine='xlsxwriter')
    workbook = writer.book
    
    # Define formats
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4472C4',
        'font_color': 'white',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })
    
    subheader_format = workbook.add_format({
        'bold': True,
        'bg_color': '#D9E2F3',
        'border': 1
    })
    
    number_format = workbook.add_format({
        'num_format': '0.0000',
        'border': 1
    })
    
    # Write metadata sheet first
    metadata_df = pd.DataFrame(
        list(metadata.items()),
        columns=['Parameter', 'Value']
    )
    metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
    
    # Process each data sheet
    sheet_order = [
        'raw_data',
        'mean_values', 
        'delta_ct',
        'pow2_delta_ct',
        'normalized_fold_change',
        'statistics',
        'qc_summary'
    ]
    
    for sheet_name in sheet_order:
        if sheet_name in data and data[sheet_name] is not None:
            df = data[sheet_name]
            
            # Special handling for multi-level columns (Mean/SD format)
            if isinstance(df.columns, pd.MultiIndex):
                df.to_excel(writer, sheet_name=sheet_name.replace('_', ' ').title())
                _format_multilevel_sheet(writer, sheet_name.replace('_', ' ').title(), df)
            else:
                df.to_excel(writer, sheet_name=sheet_name.replace('_', ' ').title())
                _format_standard_sheet(writer, sheet_name.replace('_', ' ').title(), df)
    
    # Add plots if provided
    if include_plots:
        _add_plots_sheet(writer, include_plots)
    
    # Save the workbook
    writer.close()
    
    return output_path


def _format_multilevel_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Format sheets with multi-level columns (Mean/SD format).
    
    Args:
        writer: Excel writer object
        sheet_name: Name of the sheet
        df: DataFrame with multi-level columns
    """
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    
    # Formats
    mean_format = workbook.add_format({
        'num_format': '0.0000',
        'bg_color': '#E7F3E7',
        'border': 1
    })
    
    sd_format = workbook.add_format({
        'num_format': '0.0000',
        'bg_color': '#F3E7E7',
        'border': 1
    })
    
    # Apply formatting based on column level
    for col_num, col in enumerate(df.columns):
        if isinstance(col, tuple) and len(col) > 1:
            if 'Mean' in col[1]:
                worksheet.set_column(col_num + 1, col_num + 1, 12, mean_format)
            elif 'SD' in col[1]:
                worksheet.set_column(col_num + 1, col_num + 1, 12, sd_format)


def _format_standard_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Format standard sheets.
    
    Args:
        writer: Excel writer object
        sheet_name: Name of the sheet
        df: DataFrame to format
    """
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    
    # Auto-adjust column widths
    for idx, col in enumerate(df.columns):
        series = df[col]
        max_len = max((
            series.astype(str).map(len).max(),
            len(str(series.name))
        )) + 2
        worksheet.set_column(idx + 1, idx + 1, max_len)


def _add_plots_sheet(writer: pd.ExcelWriter, plot_paths: List[str]) -> None:
    """Add a sheet with embedded plots.
    
    Args:
        writer: Excel writer object
        plot_paths: List of paths to plot images
    """
    workbook = writer.book
    worksheet = workbook.add_worksheet('Plots')
    
    row = 0
    for plot_path in plot_paths:
        if os.path.exists(plot_path):
            worksheet.insert_image(row, 0, plot_path, {'x_scale': 0.5, 'y_scale': 0.5})
            row += 20  # Space between images


def build_pdf_report(
    sections: List[Dict[str, Any]],
    output_path: str,
    page_size: str = 'letter'
) -> str:
    """Build PDF report from sections.
    
    Creates a formatted PDF report with multiple sections including
    text, tables, and images.
    
    Args:
        sections: List of report sections, each containing:
            - type: 'title', 'text', 'table', 'image', 'pagebreak'
            - content: The actual content
            - style: Optional style parameters
        output_path: Output file path
        page_size: Page size ('letter' or 'A4')
        
    Returns:
        str: Path to generated PDF file
    """
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Create PDF document
    page = letter if page_size == 'letter' else A4
    doc = SimpleDocTemplate(output_path, pagesize=page)
    
    # Get styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#2b6cb0'),
        spaceAfter=30
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=colors.HexColor('#4472C4'),
        spaceAfter=12
    )
    
    # Build story
    story = []
    
    for section in sections:
        section_type = section.get('type', 'text')
        content = section.get('content', '')
        style = section.get('style', {})
        
        if section_type == 'title':
            story.append(Paragraph(content, title_style))
            story.append(Spacer(1, 0.2 * inch))
            
        elif section_type == 'heading':
            story.append(Paragraph(content, heading_style))
            
        elif section_type == 'text':
            story.append(Paragraph(content, styles['Normal']))
            story.append(Spacer(1, 0.1 * inch))
            
        elif section_type == 'table':
            table = _create_pdf_table(content, style)
            story.append(table)
            story.append(Spacer(1, 0.2 * inch))
            
        elif section_type == 'image':
            if os.path.exists(content):
                img = Image(content, width=style.get('width', 6*inch), 
                           height=style.get('height', 4*inch))
                story.append(img)
                story.append(Spacer(1, 0.2 * inch))
                
        elif section_type == 'pagebreak':
            story.append(PageBreak())
    
    # Build PDF
    doc.build(story)
    
    return output_path


def _create_pdf_table(data: pd.DataFrame, style: Dict[str, Any]) -> Table:
    """Create a formatted table for PDF.
    
    Args:
        data: DataFrame to convert to table
        style: Style parameters
        
    Returns:
        Table: Formatted table object
    """
    # Convert DataFrame to list format
    table_data = [data.columns.tolist()]  # Headers
    table_data.extend(data.values.tolist())  # Data rows
    
    # Create table
    table = Table(table_data)
    
    # Apply table style
    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ])
    
    # Zebra striping
    for i in range(1, len(table_data)):
        if i % 2 == 0:
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.HexColor('#F0F0F0'))
    
    table.setStyle(table_style)
    
    return table


def create_analysis_report(
    analysis_results: Dict[str, Any],
    experiment_name: str,
    output_dir: str
) -> Dict[str, str]:
    """Create both Excel and PDF reports from analysis results.
    
    Args:
        analysis_results: Complete analysis results
        experiment_name: Name of the experiment
        output_dir: Output directory for reports
        
    Returns:
        Dict[str, str]: Paths to generated reports
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Prepare Excel data
    excel_data = {
        'raw_data': analysis_results.get('raw_data'),
        'mean_values': analysis_results.get('mean_values'),
        'delta_ct': analysis_results.get('delta_ct'),
        'pow2_delta_ct': analysis_results.get('fold_change_data'),
        'normalized_fold_change': analysis_results.get('normalized_data'),
        'statistics': analysis_results.get('statistics'),
        'qc_summary': analysis_results.get('qc_summary')
    }
    
    # Build Excel report
    excel_path = os.path.join(output_dir, f"{experiment_name}_{timestamp}.xlsx")
    excel_path = build_xlsx_report(
        data=excel_data,
        metadata=analysis_results.get('metadata', {}),
        output_path=excel_path,
        include_plots=analysis_results.get('plot_paths', [])
    )
    
    # Prepare PDF sections
    pdf_sections = [
        {'type': 'title', 'content': f'qPCR Analysis Report: {experiment_name}'},
        {'type': 'text', 'content': f'Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'},
        {'type': 'pagebreak'},
        {'type': 'heading', 'content': 'Experiment Summary'},
        {'type': 'table', 'content': pd.DataFrame(analysis_results.get('metadata', {}).items(), 
                                                  columns=['Parameter', 'Value'])},
    ]
    
    # Add QC summary if available
    if 'qc_summary' in analysis_results:
        pdf_sections.extend([
            {'type': 'heading', 'content': 'Quality Control Summary'},
            {'type': 'table', 'content': analysis_results['qc_summary']}
        ])
    
    # Add plots
    for plot_path in analysis_results.get('plot_paths', []):
        pdf_sections.extend([
            {'type': 'pagebreak'},
            {'type': 'image', 'content': plot_path}
        ])
    
    # Build PDF report
    pdf_path = os.path.join(output_dir, f"{experiment_name}_{timestamp}.pdf")
    pdf_path = build_pdf_report(sections=pdf_sections, output_path=pdf_path)
    
    return {
        'excel': excel_path,
        'pdf': pdf_path
    }