"""
Use case: Advertisers want to generate audiences for DSP platform targeting.
Specific audience could be the users who are interested in specific car brands.
"""

import networkx as nx
from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import select, out

from nepytune import drawing


def get_root_url(g, website_url):
    """Given website url, get its root node."""
    return (
        g.V(website_url)
            .hasLabel("website")
            .in_("links_to")
    )


def brand_interaction_audience(g, website_url):
    """
    Given website url, get all transitive (through persistent) identities
    that interacted with this brand on any of its pages.
    """
    return (
        get_root_url(g, website_url)
            .out("links_to") # get all websites from this root url
            .in_("visited")
            .in_("has_identity").dedup()
            .out("has_identity")
            .values("uid")
    )


def draw_referenced_subgraph(g, root_url):
    graph = _build_networkx_graph(
        root_url,
        query_results=_get_transient_ids(
            _get_persistent_ids_which_visited_website(g, root_url),
            root_url
        ).next()
    )
    graph = drawing.layout(graph, nx.kamada_kawai_layout)
    drawing.draw(
        title="Brand interaction",
        scatters=[
            drawing.edges_scatter(graph)
        ] + list(
            drawing.scatters_by_label(
                graph, attrs_to_skip=["pos"],
                sizes={"websiteGroup": 30, "transientId": 10, "persistentId": 15, "website": 10}
            )
        ),
    )


# ===========================
# Everything below was added to introspect the query results via visualisations


def _build_networkx_graph(root_url, query_results):
    graph = nx.Graph()
    graph.add_node(
        root_url, label="websiteGroup", url=root_url
    )

    for persistent_id, visited_events in query_results.items():
        graph.add_node(persistent_id, label="persistentId", pid=persistent_id)

        for event in visited_events:
            graph.add_node(event["uid"], label="transientId", uid=event["uid"])
            if event["visited_url"] != root_url:
                graph.add_node(event["visited_url"], label="website", url=event["visited_url"])
                graph.add_edge(event["uid"], event["visited_url"], label="visited")
            graph.add_edge(persistent_id, event["uid"], label="has_identity")
            graph.add_edge(root_url, event["visited_url"], label="links_to")

    return graph


def _get_persistent_ids_which_visited_website(g, root_url):
    return (
        g.V(root_url)
            .aggregate("root_url")
            .in_("visited")
            .in_("has_identity").dedup().limit(50).fold()
            .project("root_url", "persistent_ids")
                .by(select("root_url").unfold().valueMap(True))
                .by()
    )


def _get_transient_ids(query, root_url):
    return (
        query
        .select("persistent_ids")
        .unfold()
        .group()
            .by("pid")
            .by(
                out("has_identity")
                .outE("visited")
                .has(  # do not go through links_to, as it causes neptune memory errors
                    "visited_url", P.between(root_url, root_url + "/zzz")
                )
                .valueMap("uid", "visited_url")
                .dedup()
                .limit(15)
                .fold()
            )
    )
