"""
Use case: user has visited a travel agency website recently.
Advertisers want to display ads about travel promotions to all members of his household.
"""
from collections.abc import Iterable

from gremlin_python.process.traversal import Column, T
from gremlin_python.process.graph_traversal import project, valueMap, out
import networkx as nx

from nepytune import drawing


def get_all_transient_ids_in_household(g, transient_id):
    """Given transient id, get all transient ids from its household."""
    return (
        g.V(transient_id)
            .hasLabel("transientId")
            .in_("has_identity")
            .in_("member")
            .has("type", "household")
            .out("member")
            .out("has_identity").
        values("uid")
    )


def draw_referenced_subgraph(g, transient_id):
    graph = drawing.spring_layout(
        _build_networkx_graph(
            g,
            g.V(transient_id).in_("has_identity").in_("member").next()
        )
    )

    drawing.draw(
        title="Single identity group graph structure",
        scatters=[
            drawing.edges_scatter(graph)
        ] + list(
            drawing.scatters_by_label(
                graph, attrs_to_skip=["pos"],
                sizes={"identityGroup": 60, "transientId": 20, "persistentId": 40}
            )
        ),
        annotations=drawing.edge_annotations(graph)
    )


# ===========================
# Everything below was added to introspect the query results via visualisations


def _get_identity_group_hierarchy(g, identity_group_id):
    return (
        g.V(identity_group_id)
            .project("props", "persistent_ids")
                .by(valueMap(True))
                .by(
                    out("member")
                    .group()
                    .by()
                    .by(
                        project("props", "transient_ids")
                            .by(valueMap(True))
                            .by(
                                out("has_identity").valueMap(True).fold()
                        )
                    ).select(Column.values)
                )
    )


def _build_networkx_graph(g, identity_group_id):
    def get_attributes(attribute_list):
        attrs = {}
        for attr_name, value in attribute_list:
            attr_name = str(attr_name)

            if isinstance(value, Iterable) and not isinstance(value, str):
                for i, single_val in enumerate(value):
                    attrs[f"{attr_name}-{i}"] = single_val
            else:
                if '.' in attr_name:
                    attr_name = attr_name.split('.')[-1]
                attrs[attr_name] = value

        return attrs

    graph = nx.Graph()

    for ig_node in _get_identity_group_hierarchy(g, identity_group_id).toList():
        ig_id = ig_node["props"][T.id]

        graph.add_node(
            ig_id,
            **get_attributes(ig_node["props"].items())
        )

        for persistent_node in ig_node["persistent_ids"]:
            p_id = persistent_node["props"][T.id]
            graph.add_node(
                p_id,
                **get_attributes(persistent_node["props"].items())
            )
            graph.add_edge(ig_id, p_id, label="member")

            for transient_node_map in persistent_node["transient_ids"]:
                transient_id = transient_node_map[T.id]
                graph.add_node(
                    transient_id,
                    **get_attributes(transient_node_map.items())
                )
                graph.add_edge(transient_id, p_id, label="has_identity")

    return graph
