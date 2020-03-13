import plotly.graph_objects as go
import colorlover as cl

def make_bars(data, title, x_title, y_title, lazy=False):
    color = cl.scales[str(len(data.keys()))]['div']['RdYlBu']
    fig = go.Figure(
        [
            go.Bar(
                x=list(data.keys()),
                y=list(data.values()),
                hoverinfo="y",
                marker=dict(color=color),
            )
        ]
    )

    fig.update_layout(
        title=title,
        yaxis_type="log",
        xaxis_title=x_title,
        yaxis_title=y_title,
    )
    if not lazy:
        fig.show()
    return fig