import plotly.graph_objects as go


def show(data):
    type_labels, type_values = zip(*data["type"].items())
    device_labels, device_values = zip(*data["device"].items())
    browser_labels, browser_values = zip(*data["browser"].items())

    trace = go.Sunburst(
        labels=type_labels + device_labels + browser_labels,
        parents=["", ""] + ["device"] * len(device_labels) + ["cookie"] * len(browser_labels),
        values=type_values + device_values + browser_values,
        hoverinfo="label+value",
    )

    layout = go.Layout(
        margin=go.layout.Margin(t=0, l=0, r=0, b=0),
    )

    fig = go.Figure([trace], layout)
    fig.show()
