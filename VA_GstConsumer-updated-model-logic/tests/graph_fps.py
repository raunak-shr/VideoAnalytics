import os
from datetime import datetime
from collections import defaultdict
import glob
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots

LOG_FOLDER = r"C:\VA\GstProducer\logs"

def process_log_file(file_path):
    frame_counts = defaultdict(int)
    
    with open(file_path, 'r') as file:
        for line in file:
            timestamp_str = line.split(',')[0]
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
            
            time_key = timestamp.strftime('%H:%M:%S')
            
            frame_counts[time_key] += 1
    
    sorted_counts = sorted(frame_counts.items(), key=lambda x: x[0])
    return sorted_counts

def get_log_files():
    log_files = glob.glob(os.path.join(LOG_FOLDER, '*_frames.log'))
    return [(os.path.basename(f).replace('_frames.log', ''), f) for f in log_files]

def calculate_average_fps(frame_counts):
    total_frames = sum(count for _, count in frame_counts)
    total_seconds = len(frame_counts)
    return total_frames / total_seconds if total_seconds > 0 else 0

load_figure_template("lux")
external_stylesheets = [dbc.themes.LUX]
app = Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1('Log Analysis: Frames Sent Per Second by Location'),
    dcc.Dropdown(
        id='location-dropdown',
        options=[{'label': name, 'value': file_path} 
                 for name, file_path in get_log_files()],
        value=get_log_files()[0][1] if get_log_files() else None,
        placeholder="Select a location"
    ),
    dcc.Graph(id='frame-count-graph')
])

@app.callback(
    Output('frame-count-graph', 'figure'),
    Input('location-dropdown', 'value')
)
def update_graph(file_path):
    if not file_path:
        return go.Figure()  # Return empty figure if no file selected

    sorted_counts = process_log_file(file_path)
    
    times = [item[0] for item in sorted_counts]
    frame_counts = [item[1] for item in sorted_counts]
    
    avg_fps = calculate_average_fps(sorted_counts)
    
    location_name = os.path.basename(file_path).replace('_frames.log', '')
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1,
                        subplot_titles=("Bar Plot", "Line Plot"))

    fig.add_trace(
        go.Bar(x=times, y=frame_counts, name="Frame Count"),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(x=times, y=frame_counts, mode='lines', name="Frame Count"),
        row=2, col=1
    )

    fig.update_layout(
        title=f'Number of Frames Sent Per Second - {location_name} (Avg FPS: {avg_fps:.2f})',
        height=800,  
        xaxis_title='Time',
        showlegend=False
    )

    fig.update_yaxes(title_text="Number of Frames", row=1, col=1)
    fig.update_yaxes(title_text="Number of Frames", row=2, col=1)

    fig.update_xaxes(tickangle=-90)
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)