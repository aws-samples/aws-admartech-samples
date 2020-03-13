import itertools

import plotly.graph_objects as go
import networkx as nx


def layout(graph, layout=nx.spring_layout, **layout_args):
    pos = layout(graph, **layout_args)

    nx.set_node_attributes(graph, {
        node_id: {
            "pos": value
        }
        for node_id, value in pos.items()
    })
    return graph


def spring_layout(graph):
    return layout(graph, nx.spring_layout, scale=0.5)


def group_by_label(graph, type_="nodes"):
    if type_ == "nodes":
        return group_by_grouper(graph, lambda x: x[1]["label"], type_)
    else:
        return group_by_grouper(graph, lambda x: x[2]["label"], type_)


def group_by_grouper(graph, grouper, type_="nodes"):
    if type_ == "nodes":
        data = graph.nodes(data=True)
    else:
        data = graph.edges(data=True)

    return itertools.groupby(
        sorted(list(data), key=grouper),
        key=grouper
    )


def edges_scatter(graph):
    edge_x = []
    edge_y = []

    for edge in graph.edges():
        x0, y0 = graph.node[edge[0]]["pos"]
        x1, y1 = graph.node[edge[1]]["pos"]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    return go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        name="edges",
        hoverinfo="none",
        mode="lines",
    )


def edge_scatters_by_label(graph, widths=None, colors=None, dashes=None, opacity=None):
    if not colors:
        colors = {}
    if not dashes:
        dashes = {}
    if not widths:
        widths = {}
    if not opacity:
        opacity = {}

    for label, edges in group_by_label(graph, type_="edges"):
        edge_x = []
        edge_y = []

        for edge in edges:
            x0, y0 = graph.node[edge[0]]["pos"]
            x1, y1 = graph.node[edge[1]]["pos"]
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)

        yield go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(
                width=widths.get(label, 0.5),
                color=colors.get(label, '#888'),
                dash=dashes.get(label, "solid")
            ),
            opacity=opacity.get(label, 1),
            name=label,
            hoverinfo="none",
            mode="lines",
        )



def edge_annotations(graph):
    annotations = []
    for from_, to_, attr_map in graph.edges(data=True):
        x0, y0 = graph.node[from_]["pos"]
        x1, y1 = graph.node[to_]["pos"]
        x_mid, y_mid = (x0 + x1) / 2, (y0 + y1) / 2
        annotations.append(dict(
            xref="x",
            yref="y",
            x=x_mid, y=y_mid,
            text=attr_map["label"],
            font=dict(size=12),
            showarrow=False
        ))

    return annotations


def scatters_by_label(graph, attrs_to_skip, sizes=None, colors=None):
    if not colors:
        colors = {}
    if not sizes:
        sizes = {}

    for i, (label, node_group) in enumerate(group_by_label(graph)):
        node_group = list(node_group)
        node_x = []
        node_y = []
        opacity = []
        size_list = []

        for node_id, _ in node_group:
            x, y = graph.node[node_id]["pos"]
            opacity.append(graph.node[node_id].get("opacity", 1))
            size_list.append(
                graph.node[node_id].get("size", sizes.get(label, 10))
            )
            node_x.append(x)
            node_y.append(y)

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            name=label,
            mode='markers',
            hoverinfo='text',
            marker=dict(
                showscale=False,
                colorscale='Hot',
                reversescale=True,
                color=colors.get(label, i * 5),
                opacity=opacity,
                size=size_list,
                line_width=2
            )
        )

        node_text = []

        def format_v(attr, value):
            if isinstance(value, dict):
                return "".join([format_v(k, str(v)) for k, v in value.items()])
            value = str(value)
            if len(value) < 80:
                return f"</br>{attr}: {value}"
            else:
                result = f"</br>{attr}: "
                substr = ""
                for word in value.split(" "):
                    if len(word + substr) < 80:
                        substr = f"{substr} {word}"
                    else:
                        result = f"{result} </br> {5 * ' '} {substr}"
                        substr = ""

                return f"{result} </br> {5 * ' '} {substr}"

        for node_id, attr_dict in node_group:
            node_text.append(
                "".join([
                    format_v(attr, value) for attr, value in attr_dict.items()
                    if attr not in attrs_to_skip
                ])
            )

        node_trace.text = node_text

        yield node_trace


def draw(title, scatters, annotations=None):
    fig = go.Figure(
        data=scatters,
        layout=go.Layout(
            title_text=title,
            titlefont_size=16,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    )
    if annotations:
        fig.update_layout(
            annotations=annotations
        )
    fig.show()
