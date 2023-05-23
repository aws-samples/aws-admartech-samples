"""
Use case:

Identify look-alike customers for a product.
The goal here is to identify prospects, who show similar behavioral patterns as your existing customers.
While we can easily do this algorithmically and automate this, the goal here is to provide visual query
to improve human understanding to the marketing analysts.
What are the device ids from my customer graph, who are not yet buying my product (say Golf Club),
but are show similar behavior patterns such lifestyle choices of buying golf or other sporting goods.
"""

from itertools import chain

import networkx as nx

from gremlin_python.process.graph_traversal import select, out, choose, constant, or_, group
from gremlin_python.process.traversal import Column, Order, P

import plotly.graph_objects as go

from nepytune import drawing


def recommend_similar_audience(g, website_url, categories_limit=3, search_time_limit_in_seconds=15):
    """Given website url, categories_limit, categories_coin recommend similar audience in n most popular categories.

    Similar audience - audience of users that at least once visited subpage of domain that contains IAB-category codes
    that are most popular across users of given website
    """
    average_guy = (
        g.V(website_url)
            .in_("visited")
            .in_("has_identity").dedup()
            .hasLabel("persistentId")
            .group().by()
            .by(
                out("has_identity").out("visited").in_("links_to")
                .groupCount().by("categoryCode")
            )
            .select(Column.values).unfold().unfold()
            .group().by(Column.keys)
            .by(select(Column.values).mean()).unfold()
            .order().by(Column.values, Order.desc)
            .limit(categories_limit)
    )

    most_popular_categories = dict(chain(*category.items()) for category in average_guy.toList())

    guy_stats_subquery = (
        out("has_identity")
        .out("visited").in_("links_to")
        .groupCount().by("categoryCode")
        .project(*most_popular_categories.keys())
    )

    conditions_subqueries = []
    for i in most_popular_categories:
        guy_stats_subquery = guy_stats_subquery.by(choose(select(i), select(i), constant(0)))
        conditions_subqueries.append(
            select(Column.values).unfold()
                .select(i)
                .is_(P.gt(int(most_popular_categories[i])))
        )

    return (
            g.V()
                .hasLabel("websiteGroup")
                .has("categoryCode", P.within(list(most_popular_categories.keys())))
                .out("links_to").in_("visited").dedup().in_("has_identity").dedup()
                .hasLabel("persistentId")
                .where(
                    out("has_identity").out("visited")
                           .has("url", P.neq(website_url))
                )
                .timeLimit(search_time_limit_in_seconds * 1000)
                .local(
                    group().by().by(guy_stats_subquery)
                    .where(or_(*conditions_subqueries))
                )
                .select(Column.keys).unfold()
                .out("has_identity")
                .values("uid")
    )


def draw_average_buyer_profile_pie_chart(g, website_url, categories_limit=3,):
    average_profile = _get_categories_popular_across_audience_of_website(
        g, website_url, categories_limit=categories_limit
    ).toList()
    average_profile = dict(chain(*category.items()) for category in average_profile)

    labels = list(average_profile.keys())
    values = list(int(i) for i in average_profile.values())

    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0)])
    fig.update_traces(textinfo='value+label+percent')
    fig.update_layout(
        title_text=f"3 Most popular IAB categories of "
                   f"<b>\"Average Buyer Profile\"</b>"
                   f"<br>for thank you page <b>{website_url}</b>")
    fig.show()


def draw_referenced_subgraph(g, website_url, categories_limit=3, search_time_limit_in_seconds=15):
    average_profile = _get_categories_popular_across_audience_of_website(
        g, website_url, categories_limit=categories_limit
    ).toList()
    average_profile = dict(
        chain(*category.items()) for category in average_profile
    )
    similar_audience = _query_users_activities_stats(
        g, website_url, average_profile, search_time_limit_in_seconds=search_time_limit_in_seconds
    )
    similar_audience = similar_audience.limit(15).toList()

    graph = _build_graph(average_profile, similar_audience)

    iabs = [n for n, params in graph.nodes(data=True) if params["label"] == "IAB"]
    avg_iabs = [n for n in iabs if graph.nodes[n]["category"] in average_profile]

    graph_with_pos_computed = drawing.layout(
        graph,
        nx.shell_layout,
        nlist=[
            ["averageBuyer"],
            avg_iabs,
            set(iabs) - set(avg_iabs),
            [n for n, params in graph.nodes(data=True) if params["label"] == "persistentId"],
            [n for n, params in graph.nodes(data=True) if params["label"] == "transientId"],
        ]
    )

    # update positions
    for name in set(iabs) - set(avg_iabs):
        node = graph_with_pos_computed.nodes[name]
        node["pos"] = [node["pos"][0], node["pos"][1]-1.75]

    for name in ["averageBuyer"] + avg_iabs:
        node = graph_with_pos_computed.nodes[name]
        node["pos"] = [node["pos"][0], node["pos"][1]+1.75]

    node = graph_with_pos_computed.nodes["averageBuyer"]
    node["pos"] = [node["pos"][0], node["pos"][1]+1]

    drawing.draw(
        title="User devices that visited ecommerce websites and optionally converted",
        scatters=list(
            drawing.edge_scatters_by_label(
                graph_with_pos_computed,
                dashes={
                    "interestedInButNotSufficient": "dash",
                    "interestedIn": "solid"
                }
            )) + list(
            drawing.scatters_by_label(
                graph_with_pos_computed, attrs_to_skip=["pos", "opacity"],
                sizes={
                    "averageBuyer": 30,
                    "IAB":10,
                    "persistentId":20
                }
            )
        )
    )


# ===========================
# Everything below was added to introspect the query results via visualisations

def _get_categories_popular_across_audience_of_website(g, website_url, categories_limit=3):
    return (
        g.V(website_url)
            .in_("visited")
            .in_("has_identity").dedup()
            .hasLabel("persistentId")
            .group().by()
            .by(
            out("has_identity").out("visited").in_("links_to")
                .groupCount().by("categoryCode")
        )
            .select(Column.values).unfold().unfold()
            .group().by(Column.keys)
            .by(select(Column.values).mean()).unfold()
            .order().by(Column.values, Order.desc)
            .limit(categories_limit)
    )


def _query_users_activities_stats(g, website_url, most_popular_categories,
                                  search_time_limit_in_seconds=30):
     return (
        g.V()
            .hasLabel("websiteGroup")
            .has("categoryCode", P.within(list(most_popular_categories.keys())))
            .out("links_to").in_("visited").dedup().in_("has_identity").dedup()
            .hasLabel("persistentId")
            .where(
            out("has_identity").out("visited")
                .has("url", P.neq(website_url))
        )
            .timeLimit(search_time_limit_in_seconds * 1000)
            .local(
                group().by().by(
                    out("has_identity")
                    .out("visited").in_("links_to")
                    .groupCount().by("categoryCode")
                )
                .project("pid", "iabs", "tids")
                .by(select(Column.keys).unfold())
                .by(select(Column.values).unfold())
                .by(select(Column.keys).unfold().out("has_identity").values("uid").fold())
        )
    )


def _build_graph(average_buyer_categories, similar_audience):
    avg_buyer = "averageBuyer"

    graph = nx.Graph()
    graph.add_node(avg_buyer, label=avg_buyer, **average_buyer_categories)

    for avg_iab in average_buyer_categories.keys():
        graph.add_node(avg_iab, label="IAB", category=avg_iab)
        graph.add_edge(avg_buyer, avg_iab, label="interestedIn")

    for user in similar_audience:
        pid, cats, tids = user["pid"], user["iabs"], user["tids"]

        user_categories = dict(sorted(cats.items(), key=lambda x: x[1])[:3])
        comparison = {k: cats.get(k, 0) for k in average_buyer_categories.keys()}
        user_categories.update(comparison)

        user_comparisons = False
        for ucategory, value in user_categories.items():
            graph.add_node(ucategory, label="IAB", category=ucategory)
            label = "interestedIn"
            if value:
                if ucategory in average_buyer_categories:
                    if user_categories[ucategory] >= average_buyer_categories[ucategory]:
                        user_comparisons = True
                    else:
                        label = "interestedInButNotSufficient"
                graph.add_edge(pid, ucategory, label=label)

        opacity = 1 if user_comparisons else 0.5
        for tid in tids:
            graph.add_edge(pid, tid, label="hasIdentity")
            graph.add_node(tid, label="transientId", uid=tid, opacity=opacity)

        graph.add_node(
            pid, label="persistentId", pid=pid,
            opacity=opacity, **cats
        )

    return graph

