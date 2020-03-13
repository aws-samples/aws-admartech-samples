import colorlover as cl
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def show(data):
    type_labels, type_values = zip(*data["type"].items())
    device_labels, device_values = zip(*data["device"].items())
    browser_labels, browser_values = zip(*data["browser"].items())

    fig = make_subplots(rows=3, cols=1, specs=[
        [{"type": "pie"}],
        [{"type": "pie"}],
        [{"type": "pie"}]
    ])

    fig.add_trace(
        go.Pie(labels=list(reversed(type_labels)), values=list(reversed(type_values)), hole=0, name="Type",
               marker={'colors': ['#7F7FFF', '#FF7F7F']},
               textinfo='label+percent', hoverinfo="label+percent+value", textfont_size=20
               ),
        row=2, col=1,

    )

    fig.add_trace(
        go.Pie(labels=["device <br> type"], values=[data["type"]["device"]],
               hole=0, textinfo='label', hoverinfo="label+value",
               marker={'colors': ['#7F7FFF']}, textfont_size=20
               ),
        row=1, col=1,

    )

    fig.add_trace(
        go.Pie(labels=device_labels, values=device_values, hole=.8, opacity=1,
               textinfo='label', textposition='outside', hoverinfo="label+percent+value",
               marker={'colors': ['rgb(247,251,255)',
                                  'rgb(222,235,247)',
                                  'rgb(198,219,239)',
                                  'rgb(158,202,225)',
                                  'rgb(107,174,214)',
                                  'rgb(66,146,198)',
                                  'rgb(33,113,181)',
                                  'rgb(8,81,156)',
                                  'rgb(8,48,107)',
                                  'rgb(9,32,66)',
                                 ]
                      }, textfont_size=12),
        row=1, col=1,
    )

    fig.add_trace(
        go.Pie(labels=["cookie <br> browser"], values=[data["type"]["cookie"]],
               hole=0, textinfo='label', hoverinfo="label+value",
               marker={'colors': ['#FF7F7F']}, textfont_size=20),
        row=3, col=1,
    )

    fig.add_trace(
        go.Pie(labels=browser_labels, values=browser_values, hole=.8,
               textinfo='label', textposition='outside', hoverinfo="label+percent+value",
               marker={'colors': ['rgb(255,245,240)',
                                  'rgb(254,224,210)',
                                  'rgb(252,187,161)',
                                  'rgb(252,146,114)',
                                  'rgb(251,106,74)',
                                  'rgb(239,59,44)',
                                  'rgb(203,24,29)',
                                  'rgb(165,15,21)',
                                  'rgb(103,0,13)',
                                  'rgb(51, 6,12)'
                                 ]
                      }, textfont_size=12),
        row=3, col=1,
    )

    fig.update_layout(
        showlegend=False,
        height=1000,
    )

    fig.show()
