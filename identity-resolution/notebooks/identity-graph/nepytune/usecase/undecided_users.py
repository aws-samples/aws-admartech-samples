"""
Use case: Ecommerce publishers want to convince undecided users to purchase the product by offering them discount codes
as soon as they have met certain criteria. Find all users who have visited product page at least X times in the last
30 days, but did not buy anything (have not visited thank you page).
"""
from collections import Counter

from gremlin_python.process.traversal import P, Column
from gremlin_python.process.graph_traversal import (
    has, groupCount,
    constant, and_, coalesce, select, count, out, where, values
)

import networkx as nx

from nepytune import drawing


def undecided_user_audience_check(g, transient_id, website_url, thank_you_page_url, since, min_visited_count):
    """
    Given transient id, check whether it belongs to an audience.

    It's simple yes, no question.

    User belongs to an audience whenever all of the following criteria are met:
        * visited some website url at least X times since specific timestamp
        * did not visit thank you page url since specific timestamp
    """
    return (
        g.V(transient_id)
            .hasLabel("transientId")
            .in_("has_identity")
            .out("has_identity")
            .outE("visited")
            .has("ts", P.gt(since))
            .choose(
                has("visited_url", website_url),
                groupCount("visits").by(constant("page_visits"))
            )
            .choose(
                has("visited_url", thank_you_page_url),
                groupCount("visits").by(constant("thank_you_page_vists"))
            )
            .cap("visits")
            .coalesce(
                and_(
                    coalesce(select("thank_you_page_vists"), constant(0)).is_(0),
                    select("page_visits").is_(P.gt(min_visited_count))
                ).choose(
                    count().is_(1),
                    constant(True)
                ),
                constant(False)
            )

    )


def undecided_users_audience(g, website_url, thank_you_page_url, since, min_visited_count):
    """
    Given website url, get all the users that meet audience conditions.

    It returns list of transient identities uids.

    Audience is build from the users that met following criteria:
        * visited some website url at least X times since specific timestamp
        * did not visit thank you page url since specific timestamp
    """
    return (
        g.V(website_url)
            .hasLabel("website")
            .inE("visited").has("ts", P.gt(since)).outV()
            .in_("has_identity")
            .groupCount()
            .unfold().dedup()
            .where(
                select(Column.values).is_(P.gt(min_visited_count))
            )
            .select(Column.keys).as_("pids")
            .map(
                out("has_identity")
                .outE("visited")
                .has("visited_url", thank_you_page_url)
                .has("ts", P.gt(since)).outV()
                .in_("has_identity").dedup()
                .values("pid").fold()
            ).as_("pids_that_visited")
            .select("pids")
            .not_(
                has("pid", where(P.within("pids_that_visited")))
            )
            .out("has_identity")
                .values("uid")
    )


def draw_referenced_subgraph(g, website_url, thank_you_page_url, since, min_visited_count):
    raw_graph = _build_networkx_graph(g, website_url, thank_you_page_url, since)

    persistent_nodes = [node for node, attr in raw_graph.nodes(data=True) if attr["label"] == "persistentId"]
    graph_with_pos_computed = drawing.layout(
        raw_graph,
        nx.shell_layout,
        nlist=[
            [website_url],
            [node for node, attr in raw_graph.nodes(data=True) if attr["label"] == "transientId"],
            [node for node, attr in raw_graph.nodes(data=True) if attr["label"] == "persistentId"],
            [thank_you_page_url]
        ]
    )

    # update positions and change node label
    raw_graph.nodes[thank_you_page_url]["pos"] += (0, 0.75)
    for node in persistent_nodes:
        has_visited_thank_you_page = False
        visited_at_least_X_times = False
        for check_name, value in raw_graph.nodes[node]["visited_events"].items():
            if ">=" in check_name and value > 0:
                if "thank" in check_name:
                    has_visited_thank_you_page = True
                elif value > min_visited_count:
                    visited_at_least_X_times = True
        if (has_visited_thank_you_page or not visited_at_least_X_times):
            for _, to in raw_graph.edges(node):
                raw_graph.nodes[to]["opacity"] = 0.25
            raw_graph.nodes[node]["opacity"] = 0.25

    drawing.draw(
        title="User devices that visited ecommerce websites and optionally converted",
        scatters=[
            drawing.edges_scatter(graph_with_pos_computed)
        ] + list(
            drawing.scatters_by_label(
                graph_with_pos_computed, attrs_to_skip=["pos", "opacity"],
                sizes={
                    "transientId": 10, "transientId-audience": 10,
                    "persistentId": 20, "persistentId-audience": 20,
                    "website": 30,
                    "thankYouPage": 30,
                }
            )
        )
    )


# ===========================
# Everything below was added to introspect the query results via visualisations


def _get_subgraph(g, website_url, thank_you_page_url, since):
    return (
        g.V()
        .hasLabel("website")
        .has("url", P.within([website_url, thank_you_page_url]))
        .in_("visited")
        .in_("has_identity")
        .dedup().limit(20)
        .project("persistent_id", "transient_ids", "visited_events")
            .by(values("pid"))
            .by(out("has_identity").values("uid").fold())
            .by(
                out("has_identity")
                .outE("visited")
                .has("visited_url", P.within([website_url, thank_you_page_url]))
                .valueMap("visited_url", "ts", "uid").dedup().fold()
            )
    )


def _build_networkx_graph(g, website_url, thank_you_page_url, since):
    graph = nx.Graph()
    graph.add_node(website_url, label="website", url=website_url)
    graph.add_node(thank_you_page_url, label="thankYouPage", url=thank_you_page_url)

    for data in _get_subgraph(g, website_url, thank_you_page_url, since).toList():
        graph.add_node(data["persistent_id"], label="persistentId", pid=data["persistent_id"],
                       visited_events=Counter())

        for transient_id in data["transient_ids"]:
            graph.add_node(transient_id, label="transientId", uid=transient_id, visited_events=Counter())
            graph.add_edge(transient_id, data["persistent_id"], label="has_identity")

        for event in data["visited_events"]:
            edge = event["visited_url"], event["uid"]
            try:
                graph.edges[edge]["ts"].append(event["ts"])
            except:
                graph.add_edge(*edge, label="visited", ts=[event["ts"]])


            for node_map in graph.nodes[data["persistent_id"]], graph.nodes[event["uid"]]:
                if event["visited_url"] == website_url:
                    node_map["visited_events"][f"visited website < {since}"] += (event["ts"] < since)
                    node_map["visited_events"][f"visited website >= {since}"] += (event["ts"] >= since)
                else:
                    node_map["visited_events"][f"visited thank you page < {since}"] += (event["ts"] < since)
                    node_map["visited_events"][f"visited thank you page >= {since}"] += (event["ts"] >= since)

    return graph
