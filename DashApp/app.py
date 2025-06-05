from dash import Dash, html, dcc
import pandas as pd
import os
import plotly.express as px
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template


def quarter_to_time(quarter):
    start_minute = (quarter - 1) * 15
    end_minute = start_minute + 15
    start_time = f"{start_minute // 60:02d}:{start_minute % 60:02d}"
    end_time = f"{end_minute // 60:02d}:{end_minute % 60:02d}"
    return f"{start_time} - {end_time}"


def info_exporter(df):
    location_id = df.at[0, 'location_id'].split('_')[0]
    date = df.at[0, 'Date']

    # time = '6:00 A.M. - 6:00 P.M.'
    # time_14_15 = '6:00 A.M. - 1:00 P.M.'
    # time_20 = '6:00 A.M. - 11:45 A.M.'
    # if int(df.at[0, 'location_id'].split('-')[-1]) == 14 or 15:
    #     time = time_14_15
    # elif int(df.at[0, 'location_id'].split('-')[-1]) == 20:
    #     time = time_20

    row = dbc.Row([
        html.H2(f'Location : {location_id}', className="text-primary text-center fs-3"),
        html.H2(f'Date : {date}', className="text-primary text-center fs-3"),  # Time: {time}
    ])
    return row


def card_exporter(df):
    df['minute'] = df['Hour'] * 60 + (df['Quarter'] - 1) * 15

    # Summary Cards data
    vehicle_counts = df.groupby('vehicle_id')['count'].sum().reset_index()
    total_vehicles = vehicle_counts['count'].sum()  # Total count of all vehicles
    vehicle_counts['vehicle_id'] = vehicle_counts['vehicle_id'].replace('motorcycle', 'Bike')
    # Individual vehicle type cards
    vehicle_cards = [
        dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.H2(f'{x["vehicle_id"].capitalize()}', className="text-center text-success"),
                        html.H2(f'{x["count"]}', className="text-center text-success")
                    ]
                ),
                className="mb-4",
            ),
            width=2  # Adjust the width as needed
        )
        for x in vehicle_counts[["vehicle_id", "count"]].to_dict("records")
    ]

    # Total vehicles card
    total_card = dbc.Col(
        dbc.Card(
            dbc.CardBody(
                [
                    html.H2('Total', className="text-center text-primary"),
                    html.H2(f'{total_vehicles}', className="text-center text-primary")
                ]
            ),
            className="mb-4",
        ),
        width=2  # Adjust the width as needed
    )

    # Combine individual cards and total card into a row
    row = dbc.Row(
        vehicle_cards + [total_card],
        justify='center',
    )

    return row


def bar_exporter(df):

    df_copy = df.copy()

    df_copy['Continuous_Quarter'] = df_copy['Hour'] * 4 + df_copy['Quarter']

    grouped_df_total = df_copy.groupby('Continuous_Quarter')['count'].sum().reset_index()
    grouped_df_total['Continuous_Quarter'] = grouped_df_total['Continuous_Quarter'].apply(quarter_to_time)
    # Create the bar plot
    fig_total = px.bar(grouped_df_total, x='Continuous_Quarter', y='count',
                       title='Vehicle Distribution in Every 15-mins',
                       labels={'count': 'Total Count of Vehicles'},
                       text='count')
    fig_total.update_traces(textposition='outside', textfont_size=12, cliponaxis=False,
                            outsidetextfont_size=35)

    # Customize the x-axis to show each value instead of the column name
    fig_total.update_layout(
        xaxis_title=None,
        xaxis=dict(
            tickmode='array',
            tickvals=grouped_df_total['Continuous_Quarter'],
            ticktext=grouped_df_total['Continuous_Quarter'].astype(str),
            tickangle=90,
            tickfont=dict(size=15, weight='bold'),
        )
    )
    cont = dbc.Container(
        [
            dbc.Row(dcc.Markdown(" ## Vehicle Counts in 15-mins")),
            dbc.Row(
                dcc.Graph(figure=fig_total)
            ),
            dbc.Row(
                dbc.Col(
                    html.Div(style={"height": "30px"}),
                )
            )
        ]
    )
    return cont


def line_exporter(df):
    # Create a copy of the dataframe

    df_copy = df.copy()

    df_copy['Continuous Quarter'] = df_copy['Hour'] * 4 + df_copy['Quarter']

    grouped_df_total = df_copy.groupby(['Continuous Quarter', 'vehicle_id'])['count'].sum().reset_index()
    grouped_df_total['Continuous Quarter'] = grouped_df_total['Continuous Quarter'].apply(quarter_to_time)
    # Create the bar plot
    fig_total = px.line(grouped_df_total, x='Continuous Quarter', y='count',
                        title='Total Count of Vehicles in Every Quarter',
                        labels={'count': 'Total Count of Vehicles'},
                        text='count',
                        color='vehicle_id')
    fig_total.update_traces(text=None)

    cont = dbc.Container(
        [
            dbc.Row(dcc.Markdown("## Vehicle-wise Trends in 15-min")),
            dbc.Row(dcc.Graph(figure=fig_total)),
        ]
    )
    return cont


def pie_exporter(df):
    # Pie Chart Data
    records_per_set = len(df) // 4

    # Create the 4 sets sequentially
    set1 = df.iloc[:records_per_set]
    set2 = df.iloc[records_per_set:2 * records_per_set]
    set3 = df.iloc[2 * records_per_set:3 * records_per_set]
    set4 = df.iloc[3 * records_per_set:]
    cont = dbc.Container(
        [
            dbc.Row(dcc.Markdown(" ## Vehicle Distribution by 6-Hours")),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(figure=px.pie(set1, values='count', names='vehicle_id',
                                                    title='12:00 A.M. - 6:00 A.M.').update_layout(font_size=20,))),
                    dbc.Col(dcc.Graph(figure=px.pie(set2, values='count', names='vehicle_id',
                                                    title='6:00 A.M. - 12:00 P.M.').update_layout(font_size=20,))),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(figure=px.pie(set3, values='count', names='vehicle_id',
                                                    title='12:00 P.M. - 6:00 P.M.').update_layout(font_size=20,))),
                    dbc.Col(dcc.Graph(figure=px.pie(set4, values='count', names='vehicle_id',
                                                    title='6:00 P.M. - 12:00 A.M.').update_layout(font_size=20,)))
                ]
            )
        ]
    )
    return cont


def process_subset(sub):

    info_row = info_exporter(sub)
    card_row = card_exporter(sub)
    hist_cont = bar_exporter(sub)
    line_cont = line_exporter(sub)
    # pie_cont = pie_exporter(sub)

    location_info = [info_row, card_row, hist_cont, line_cont]  #, pie_cont
    # whitespace = dbc.Row(
    #     dbc.Col(
    #         html.Div(style={"height": "240px"}),
    #     )
    # )
    # whitespace2 = dbc.Row(
    #     dbc.Col(
    #         html.Div(style={"height": "20px"}),
    #     )
    # )
    # location_info.append(whitespace)
    # location_info.insert(4, whitespace2)
    return location_info


load_figure_template("lux")
external_stylesheets = [dbc.themes.LUX]
app = Dash(__name__, external_stylesheets=external_stylesheets)

data = pd.read_excel('PTZ-01-AIRPORT.xlsx')
# Save subsets in the subsets folder
location_groups = data.groupby('location_id')
if not os.path.exists('Subsets'):
    os.mkdir('Subsets')
if len(os.listdir('Subsets')) == 0:
    idx = 0
    for location, subset in location_groups:
        idx += 1
        subset.to_csv(f'Subsets/{location}_subset{idx}.csv', index=False)

list_info = []
subsets = ['Subsets/' + x for x in os.listdir('Subsets')]
datas = [pd.read_csv(x) for x in subsets]
for subs in datas:
    loc = process_subset(subs)
    list_info.append(loc)

flattened_list = [item for sublist in list_info for item in sublist]
app.layout = dbc.Container(
    flattened_list,
)


if __name__ == '__main__':
    app.run(debug=True)
