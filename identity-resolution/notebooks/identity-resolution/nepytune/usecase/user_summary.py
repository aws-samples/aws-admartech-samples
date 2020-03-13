"""
Use case:  Advertisers want to find out information about user interests to provide an accurate targeting.
The data should be based on the activity of the user across all devices.
"""
from collections.abc import Iterable

import networkx as nx
from gremlin_python.process.traversal import Column, T
from gremlin_python.process.graph_traversal import select, out, in_, values, valueMap, project, constant

from nepytune import drawing


def get_sibling_attrs(g, transient_id):
    """
    Given transient id, get summary of information we have about it or its sibling nodes.

    We gather:
        * node attributes
        * IP / location information
        * IAB categories of visited websites
    """
    return (
        g.V(transient_id)
            .choose(
                in_("has_identity"),  # check if this transient id has persistent id
                in_("has_identity").
                    project(
                        "identity_group_id", "persistent_id", "attributes", "ip_location", "iab_categories"
                    ).by(in_("member").values("igid"))
                    .by(values("pid"))
                    .by(
                        out("has_identity").valueMap().unfold()
                        .group()
                        .by(Column.keys)
                        .by(select(Column.values).unfold().dedup().fold())
                    )
                    .by(
                        out("has_identity")
                        .out("uses").dedup().valueMap().fold()
                    )
                    .by(
                        out("has_identity")
                        .out("visited")
                        .in_("links_to")
                        .values("categoryCode").dedup().fold()
                    )
                , project(
                    "identity_group_id", "persistent_id", "attributes", "ip_location", "iab_categories"
                ).by(constant(""))
                .by(constant(""))
                .by(
                    valueMap().unfold()
                    .group()
                    .by(Column.keys)
                    .by(select(Column.values).unfold().dedup().fold())
                )
                .by(
                    out("uses").dedup().valueMap().fold()
                )
                .by(
                    out("visited")
                    .in_("links_to")
                    .values("categoryCode").dedup().fold()
                )
            )
        )


def draw_refrenced_subgraph(g, transient_id):
    raw_graph = _build_networkx_graph(
        g, g.V(transient_id).in_("has_identity").in_("member").next()
    )
    graph_with_pos_computed = drawing.layout(
        raw_graph,
        nx.spring_layout,
        iterations=2500
    )

    drawing.draw(
        title="Part of single household activity on the web",
        scatters=[
            drawing.edges_scatter(graph_with_pos_computed)
        ] + list(
            drawing.scatters_by_label(
                graph_with_pos_computed, attrs_to_skip=["pos"],
                sizes={"identityGroup": 30, "transientId": 15, "persistentId": 20, "websiteGroup": 15, "website": 10}
            )
        ),
    )


# ===========================
# Everything below was added to introspect the query results via visualisations

def _get_subgraph(g, identity_group_id):
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
                                out("has_identity")
                                .group()
                                .by()
                                .by(
                                    project("props", "ip_location", "random_website_paths")
                                    .by(valueMap(True))
                                    .by(
                                        out("uses").valueMap(True).fold()
                                    )
                                    .by(
                                        out("visited").as_("start")
                                        .in_("links_to").as_("end")
                                        .limit(100)
                                        .path()
                                            .by(valueMap("url"))
                                            .by(valueMap("url", "categoryCode"))
                                        .from_("start").to("end")
                                        .dedup()
                                        .fold()
                                    )
                                ).select(
                                    Column.values
                                )
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

    for ig_node in _get_subgraph(g, identity_group_id).toList():
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

            for transient_node in persistent_node["transient_ids"]:
                transient_node_map = transient_node["props"]
                transient_id = transient_node_map[T.id]
                graph.add_node(
                    transient_id,
                    **get_attributes(transient_node_map.items())
                )
                graph.add_edge(transient_id, p_id, label="has_identity")

                for ip_location_node in transient_node["ip_location"]:
                    ip_location_id = ip_location_node[T.id]
                    graph.add_node(ip_location_id, **get_attributes(ip_location_node.items()))
                    graph.add_edge(ip_location_id, transient_id, label="uses")

                for visited_link, root_url in transient_node["random_website_paths"]:
                    graph.add_node(visited_link["url"][0], label="website", **get_attributes(visited_link.items()))
                    graph.add_node(root_url["url"][0], label="websiteGroup", **get_attributes(root_url.items()))
                    graph.add_edge(transient_id, visited_link["url"][0], label="visits")
                    graph.add_edge(visited_link["url"][0], root_url["url"][0], label="links_to")
    return graph
