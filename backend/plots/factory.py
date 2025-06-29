

from typing import Any, Dict, Optional, Union, List
import base64
import io

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots


def generate_expression_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    plot_type: str = "bar",
    output_format: str = "json",
    error_col: Optional[str] = None,
    color_col: Optional[str] = None,
    title: Optional[str] = None,
    **kwargs: Any
) -> Union[Dict[str, Any], bytes]:
    """Generate expression plot using Plotly.
    
    Args:
        df: Input DataFrame
        x_col: X-axis column name
        y_col: Y-axis column name
        plot_type: Plot type ("bar", "box", "scatter", "violin", "heatmap")
        output_format: Output format ("json" for interactive or "png" for static)
        error_col: Column for error bars (for bar plots)
        color_col: Column for color grouping
        title: Plot title
        **kwargs: Additional plot parameters
        
    Returns:
        Union[Dict[str, Any], bytes]: Plotly JSON or PNG bytes
    """
    # Create appropriate plot based on type
    if plot_type == "bar":
        fig = _create_bar_plot(df, x_col, y_col, error_col, color_col, **kwargs)
    elif plot_type == "box":
        fig = _create_box_plot(df, x_col, y_col, color_col, **kwargs)
    elif plot_type == "scatter":
        fig = _create_scatter_plot(df, x_col, y_col, color_col, **kwargs)
    elif plot_type == "violin":
        fig = _create_violin_plot(df, x_col, y_col, color_col, **kwargs)
    elif plot_type == "heatmap":
        fig = _create_heatmap(df, x_col, y_col, **kwargs)
    else:
        raise ValueError(f"Unknown plot type: {plot_type}")
    
    # Update layout
    fig.update_layout(
        title=title or f"{y_col} by {x_col}",
        xaxis_title=kwargs.get('xaxis_title', x_col),
        yaxis_title=kwargs.get('yaxis_title', y_col),
        template=kwargs.get('template', 'plotly_white'),
        width=kwargs.get('width', 800),
        height=kwargs.get('height', 600),
        font=dict(size=kwargs.get('font_size', 12))
    )
    
    # Return in requested format
    if output_format == "json":
        return fig.to_dict()
    elif output_format == "png":
        return fig.to_image(format="png")
    elif output_format == "html":
        return fig.to_html(include_plotlyjs='cdn')
    else:
        raise ValueError(f"Unknown output format: {output_format}")


def _create_bar_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    error_col: Optional[str] = None,
    color_col: Optional[str] = None,
    **kwargs: Any
) -> go.Figure:
    """Create bar plot with optional error bars."""
    if color_col:
        # Grouped bar chart
        fig = px.bar(
            df, 
            x=x_col, 
            y=y_col, 
            color=color_col,
            error_y=error_col,
            barmode=kwargs.get('barmode', 'group')
        )
    else:
        # Simple bar chart
        fig = go.Figure()
        
        error_y = None
        if error_col and error_col in df.columns:
            error_y = dict(
                type='data',
                array=df[error_col],
                visible=True,
                thickness=2,
                width=4
            )
        
        fig.add_trace(go.Bar(
            x=df[x_col],
            y=df[y_col],
            error_y=error_y,
            marker_color=kwargs.get('color', '#2b6cb0'),
            name=y_col
        ))
    
    # Customize bar appearance
    fig.update_traces(
        marker_line_color='black',
        marker_line_width=1.5,
        opacity=kwargs.get('opacity', 0.8)
    )
    
    return fig


def _create_box_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: Optional[str] = None,
    **kwargs: Any
) -> go.Figure:
    """Create box plot with optional grouping."""
    if color_col:
        fig = px.box(df, x=x_col, y=y_col, color=color_col)
    else:
        fig = go.Figure()
        
        # Get unique x values
        x_values = df[x_col].unique()
        
        for x_val in x_values:
            y_data = df[df[x_col] == x_val][y_col]
            
            fig.add_trace(go.Box(
                y=y_data,
                name=str(x_val),
                boxpoints=kwargs.get('boxpoints', 'outliers'),
                marker_color=kwargs.get('color', '#2b6cb0')
            ))
    
    # Add individual points if requested
    if kwargs.get('show_points', False):
        fig.update_traces(
            boxpoints='all',
            jitter=0.3,
            pointpos=-1.8
        )
    
    return fig


def _create_scatter_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: Optional[str] = None,
    **kwargs: Any
) -> go.Figure:
    """Create scatter plot with optional trend line."""
    if color_col:
        fig = px.scatter(
            df, 
            x=x_col, 
            y=y_col, 
            color=color_col,
            trendline=kwargs.get('trendline', None)
        )
    else:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='markers',
            marker=dict(
                size=kwargs.get('marker_size', 8),
                color=kwargs.get('color', '#2b6cb0'),
                line=dict(width=1, color='black')
            ),
            name=y_col
        ))
        
        # Add trend line if requested
        if kwargs.get('trendline', False):
            z = np.polyfit(df[x_col], df[y_col], 1)
            p = np.poly1d(z)
            x_trend = np.linspace(df[x_col].min(), df[x_col].max(), 100)
            
            fig.add_trace(go.Scatter(
                x=x_trend,
                y=p(x_trend),
                mode='lines',
                name='Trend',
                line=dict(color='red', dash='dash')
            ))
    
    return fig


def _create_violin_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: Optional[str] = None,
    **kwargs: Any
) -> go.Figure:
    """Create violin plot."""
    if color_col:
        fig = px.violin(df, x=x_col, y=y_col, color=color_col, box=True)
    else:
        fig = go.Figure()
        
        x_values = df[x_col].unique()
        
        for x_val in x_values:
            y_data = df[df[x_col] == x_val][y_col]
            
            fig.add_trace(go.Violin(
                y=y_data,
                name=str(x_val),
                box_visible=True,
                meanline_visible=True,
                fillcolor=kwargs.get('color', '#2b6cb0'),
                opacity=0.6
            ))
    
    return fig


def _create_heatmap(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    **kwargs: Any
) -> go.Figure:
    """Create heatmap from pivot table."""
    # Pivot data for heatmap
    pivot = df.pivot_table(
        index=y_col,
        columns=x_col,
        values=kwargs.get('values_col', 'fold_change'),
        aggfunc='mean'
    )
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale=kwargs.get('colorscale', 'RdBu_r'),
        zmid=kwargs.get('zmid', 1.0),  # Center color scale at 1 for fold change
        text=np.round(pivot.values, 2),
        texttemplate='%{text}',
        textfont={"size": 10},
        hoverongaps=False
    ))
    
    return fig


def create_qpcr_dashboard(
    analysis_results: Dict[str, pd.DataFrame],
    output_format: str = "html"
) -> Union[str, Dict[str, Any]]:
    """Create comprehensive qPCR analysis dashboard.
    
    Args:
        analysis_results: Dictionary containing analysis results
        output_format: "html" or "json"
        
    Returns:
        Union[str, Dict[str, Any]]: Dashboard HTML or JSON
    """
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Mean CT Values', 'Fold Changes', 
                       'Statistical Significance', 'Expression Heatmap'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'scatter'}, {'type': 'heatmap'}]]
    )
    
    # Add plots based on available data
    if 'mean_values' in analysis_results:
        df = analysis_results['mean_values']
        # Add mean CT values plot
        # Implementation depends on data structure
    
    # Update layout
    fig.update_layout(
        title_text="qPCR Analysis Dashboard",
        showlegend=True,
        height=800,
        width=1200
    )
    
    if output_format == "html":
        return fig.to_html(include_plotlyjs='cdn')
    else:
        return fig.to_dict()


def save_plots_to_files(
    plots: Dict[str, go.Figure],
    output_dir: str,
    formats: List[str] = ['png', 'html']
) -> Dict[str, List[str]]:
    """Save multiple plots to files.
    
    Args:
        plots: Dictionary of plot names to Figure objects
        output_dir: Output directory
        formats: List of formats to save
        
    Returns:
        Dict[str, List[str]]: Dictionary of plot names to file paths
    """
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    saved_files = {}
    
    for name, fig in plots.items():
        saved_files[name] = []
        
        for fmt in formats:
            file_path = os.path.join(output_dir, f"{name}.{fmt}")
            
            if fmt == 'png':
                fig.write_image(file_path)
            elif fmt == 'html':
                fig.write_html(file_path, include_plotlyjs='cdn')
            elif fmt == 'json':
                import json
                with open(file_path, 'w') as f:
                    json.dump(fig.to_dict(), f)
            
            saved_files[name].append(file_path)
    
    return saved_files