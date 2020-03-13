import datetime
import random

import plotly.graph_objects as go
import yaml


def get_intersections(s1, s2, s3):
    class NodeElement:
        def __init__(self, **kwargs):
            self.attributes = kwargs

        def __hash__(self):
            return hash(self.attributes["persistent_id"])

        def __eq__(self, other):
            return self.attributes["persistent_id"] == other.attributes["persistent_id"]

        def __repr__(self):
            pid = self.attributes['persistent_id']
            hash_ = str(hash(self.attributes['persistent_id']))
            return f"{pid}, {hash_}"

    a = {NodeElement(**e) for e in s1}
    b = {NodeElement(**e) for e in s2}
    c = {NodeElement(**e) for e in s3}

    result = {
        "ab": a & b,
        "ac": a & c,
        "bc": b & c,
        "abc": a & b & c
    }

    result["a"] = a - (result["ab"] | result["ac"])
    result["b"] = b - (result["ab"] | result["bc"])
    result["c"] = b - (result["ac"] | result["bc"])

    return result


def make_label(node):
    return "<br>" + yaml.dump(
        node.attributes,
        default_style=None,
        default_flow_style=False,
        width=50
    ).replace("\n", "</br>")


TRIANGLES = {
    "abc": [
        [(8, -0.35), (10, -4.5), (12, -0.35)],
        [(11.51671, -2.41546), (9.98847, -4.47654), (12, -0.35)],
        [(10, 0), (12, -0.35), (8, -0.35)],
        [(8, -0.35), (8.58508, -2.5938), (9.98847, -4.47654)]
    ],
    "ab": [
        [(8, 0), (10, 4.5), (12, 0)],
        [(8, 0), (11.49694, 2.39107), (12, 0)],
        [(8, 0), (10, 4.5), (8.51494, 2.37233)],
        [(12, 0), (10, 4.5), (11.49383, 2.40639)]
    ],
    "ac": [
        [(4, -5.65), (9.7, -4.75), (7.5, -0.55)],
        [(4, -5.65), (5.26182, -2.31944), (7.5, -0.55)],
        [(8.26214, -1.9908), (8, -0.35), (7.5, -0.55)],
        [(8.92526, -3.30719), (10, -4.5), (9.7, -4.75)],
        [(7.01578, -5.9212), (4, -5.65), (9.7, -4.75)]
    ],
    "bc": [
        [(16.01075, -5.7627), (12.51157, -0.57146), (10.31157, -4.77146)],
        [(10, -4.5), (11.08632, -3.32866), (10.31157, -4.77146)],
        [(12.00131, -0.28126), (12.51157, -0.57146), (11.74943, -2.01226)],
        [(12.51157, -0.57146), (14.74975, -2.3409), (16.01075, -5.7627)],
        [(10.31157, -4.77146), (12.99579, -5.94266), (16.01157, -5.67146)]
    ],
    "a": [
        [(1.59, 4.12), (1.2, -3.54), (8.07, 5.62)],
        [(8.01, -0.31), (1.2, -3.54), (8.07, 5.62)],
        [(4.76091, -1.85498), (4.76091, -1.85498), (1.20313, -3.56193)],
        [(1.20313, -3.56193), (0, 0), (1.58563, 4.1221)],
        [(4.93073, -1.82779), (1.20313, -3.53928), (4.00216, -5.85506)],
        [(1.58563, 4.1221), (4.56262, 5.82533), (8.06809, 5.62107)],
        [(8.06809, 5.62107), (9.96902, 4.49245), (8.03037, 1.87789)],
        [(4.93976, -1.79473), (5.3695, -2.15025), (6.48177, -1.03733)],
        [(4.93976, -1.79473), (5.3695, -2.15025), (4.55188, -3.3864)],
        [(8.31901, 1.92203), (8.31901, 1.92203), (8.63932, 2.8171)],
        [(8.31901, 1.92203), (8.31901, 1.92203), (8.02962, 1.02571)]
    ],
    "b": [
        [(12.06, 5.65), (18.89, -3.51), (18.38, 4.1)],
        [(12, -0.28), (12.06, 5.65), (18.89, -3.51)],
        [(12.06077, 5.6496), (12.0047, 2.32125), (10.02248, 4.49887)],
        [(15.10229, -1.76245), (18.8895, -3.51074), (16.01075, -5.7627)],
        [(20, 0), (18.37991, 4.09718), (18.8895, -3.51074)],
        [(18.37991, 4.09718), (12.06077, 5.6496), (15.63243, 5.78919)],
    ],
    "c": [
        [(10, -12), (4.38, -8), (15.60, -8)],
        [(10, -4.55), (4.38, -8), (15.60, -8)],
        [(4.01794, -5.64598), (4.38003, -8.00561), (7.22591, -6.21212)],
        [(15.99694, -5.86975), (12.83447, -6.25924), (15.62772, -8.02776)],
        [(4.38003, -8.00561), (6.43193, -10.80379), (10, -12)],
        [(15.62772, -8.02776), (13.95762, -10.55233), (10, -12)],
        [(5.56245, -6.01624), (7.11534, -5.92534), (7.22591, -6.21212)],
        [(8.21897, -5.58699), (7.11534, -5.92534), (7.22591, -6.21212)],
        [(11.76526, -5.59305), (13.023, -5.93749), (12.83447, -6.25924)],
        [(14.49948, -5.9889), (13.023, -5.93749), (12.83447, -6.25924)],
    ],
}


def show_venn_diagram(intersections, labels):
    def point_on_triangle(pt1, pt2, pt3):
        """
        Random point on the triangle with vertices pt1, pt2 and pt3.
        """
        s, t = sorted([random.random(), random.random()])
        return (s * pt1[0] + (t - s) * pt2[0] + (1 - t) * pt3[0],
                s * pt1[1] + (t - s) * pt2[1] + (1 - t) * pt3[1])

    def area(tri):
        y_list = [tri[0][1], tri[1][1], tri[2][1]]
        x_list = [tri[0][0], tri[1][0], tri[2][0]]
        height = max(y_list) - min(y_list)
        width = max(x_list) - min(x_list)
        return height * width / 2

    empty_sets = [k for k, v in intersections.items() if not len(v)]

    if empty_sets:
        raise ValueError(f"Given intersections \"{empty_sets}\" are empty, cannot continue")

    scatters = []

    for k, v in intersections.items():
        weights = [area(triangle) for triangle in TRIANGLES[k]]
        points_pairs = [point_on_triangle(*random.choices(TRIANGLES[k], weights=weights)[0]) for _ in v]
        x, y = zip(*points_pairs)
        scatter_labels = [make_label(n) for n in v]

        scatters.append(
            go.Scatter(
                x=x,
                y=y,
                mode='markers',
                showlegend=False,
                text=scatter_labels,
                marker=dict(
                    size=10,
                    line=dict(width=2,
                              color='DarkSlateGrey'),
                    opacity=1,
                ),
                hoverinfo="text",
            )
        )
    fig = go.Figure(
        data=list(scatters),
        layout=go.Layout(
            title_text="",
            autosize=False,
            titlefont_size=16,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, scaleanchor="x", scaleratio=1)
        ),
    )

    fig.update_layout(
        shapes=[
            go.layout.Shape(
                type="circle",
                x0=0,
                y0=-6,
                x1=12,
                y1=6,
                fillcolor="Red",
                opacity=0.15,
                layer='below'
            ),
            go.layout.Shape(
                type="circle",
                x0=8,
                y0=-6,
                x1=20,
                y1=6,
                fillcolor="Blue",
                opacity=0.15,
                layer='below'
            ),
            go.layout.Shape(
                type="circle",
                x0=4,
                y0=-12,
                x1=16,
                y1=0,
                fillcolor="Green",
                opacity=0.15,
                layer='below'
            ),
        ]
    )

    fig.update_layout(
        annotations=[
            dict(
                xref="x",
                yref="y",
                x=6, y=6,
                text=labels[0],
                font=dict(size=15),
                showarrow=True,
                arrowwidth=2,
                ax=-50,
                ay=-25,
                arrowhead=7,
            ),
            dict(
                xref="x",
                yref="y",
                x=14, y=6,
                text=labels[1],
                font=dict(size=15),
                showarrow=True,
                arrowwidth=2,
                ax=50,
                ay=-25,
                arrowhead=7,
            ),
            dict(
                xref="x",
                yref="y",
                x=10, y=-12,
                text=labels[2],
                font=dict(size=15),
                showarrow=True,
                arrowwidth=2,
                ax=50,
                ay=25,
                arrowhead=7,
            ),
        ]
    )

    fig.show()
