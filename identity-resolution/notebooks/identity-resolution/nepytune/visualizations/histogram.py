import pandas as pd
import plotly.graph_objects as go


def show(activities, website_name):
    # convert activities into pandas series
    activity_series = pd.to_datetime(pd.Series(list(activities)))

    # trim timestamps to desired granulation (in this case, hours)
    hourly_activity_series = activity_series.dt.strftime("%H")

    # prepare values & labels source for histogram's xaxis 
    day_hours = pd.to_datetime(pd.date_range(start="00:00", end="23:59", freq="H"))

    # create histogram
    fig = go.Figure(
        data=[
            go.Histogram(
                x=hourly_activity_series,
                histnorm='percent'
            )
        ]
    )

    # provide titles/labels/bar_gaps
    fig.update_layout(
        title_text=f"Activity of all users that visited website <b>{website_name}<b>",
        xaxis_title_text='Day time (Hour)',
        yaxis_title_text='Percentage of visits',

        xaxis=dict(
            tickangle=45,
            tickmode='array',
            tickvals=day_hours.strftime("%H").tolist(),
            ticktext=day_hours.strftime("%H:%M").tolist()
        ),
        bargap=0.05,
    )

    # show histogram
    fig.show()
