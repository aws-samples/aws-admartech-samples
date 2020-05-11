"""
Use-Case.

Marketing analyst wants to understand path to purchase of a new product by a few early adopters ( say 5)
through interactive queries. This product is high involvement and expensive, and therefore they want to understand the
research undertaken by the customer.

* Which device was used to initiate the first research. Was that prompted by an ad, email promotion?
* How many devices were used overall and what was the time taken from initial research to final purchase
* On which devices did the customer spend more time
"""
import itertools
from collections import namedtuple, defaultdict
from datetime import timedelta

import networkx as nx
import plotly.graph_objects as go
from gremlin_python.process.traversal import P
from gremlin_python.process.graph_traversal import (
    outV, values, project, constant, select, inV, where, identity
)

from nepytune import drawing
from nepytune.visualizations import bar_plots


Event = namedtuple('Event', 'ts persistentId transientId device_type url')
Session = namedtuple('Session', 'transientId persistentId device_type events')


def get_activity_of_early_adopters(g, thank_you_page_url, skip_single_transients=False, limit=5):
    """
    Given thank you page url, find first early adopters of the product.

    In other words:
        * find first few persistent identities (or transient if they're not matched with any user)
          that visited given thank you page
        * extract their *whole* activity on the domain of the thank_you_page
    """
    return (
        g.V(thank_you_page_url)
        .hasLabel("website").as_("thank_you")
        .in_("links_to").as_("website_group")
        .select("thank_you")
        .inE("visited")
        .order().by("ts")
        .choose(
            constant(skip_single_transients).is_(P.eq(True)),
            where(outV().in_("has_identity")),
            identity()
        )
        .choose(
            outV().in_("has_identity"),
            project(
                "type", "id", "purchase_ts"
            )
                .by(constant("persistent"))
                .by(outV().in_("has_identity"))
                .by(values("ts")),
            project(
                "type", "id", "purchase_ts"
            )
                .by(constant("transient"))
                .by(outV())
                .by(values("ts"))
        ).dedup("id").limit(limit)
        .choose(
            select("type").is_("persistent"),
            project(
                "persistent_id", "transient_id", "purchase_ts"
            ).by(select("id").values("pid"))
             .by(select("id").out("has_identity").fold())
             .by(select("purchase_ts")),
            project("persistent_id", "transient_id", "purchase_ts")
                .by(constant(""))
                .by(select("id").fold())
                .by(select("purchase_ts"))
        ).project("persistent_id", "purchase_ts", "devices", "visits")
            .by(select("persistent_id"))
            .by(select("purchase_ts"))
            .by(select("transient_id").unfold().group().by(values("uid")).by(values("type")))
            .by(
                select("transient_id").unfold().outE("visited").order().by("ts")
                .where(
                    inV().in_("links_to").where(P.eq("website_group"))
                )
                .project(
                    "transientId", "url", "ts"
                ).by("uid").by("visited_url").by("ts").fold())
    )


def transform_activities(result_set):
    """Build the flat list of user activities."""
    for per_persistent_events in result_set:
        for visit in per_persistent_events["visits"]:
            if visit["ts"] <= per_persistent_events["purchase_ts"]:
                yield Event(**{
                    "persistentId": per_persistent_events["persistent_id"] or None,
                    "device_type": per_persistent_events["devices"][visit["transientId"]],
                    **visit
                })


def first_device_in_session(user_events):
    """Get device id which initialize session."""
    return user_events[0].transientId


def time_to_purchase(user_events):
    """Get device id which initialize session."""
    return user_events[-1].ts  - user_events[0].ts


def consecutive_pairs(iterable):
    f_ptr, s_ptr = itertools.tee(iterable, 2)
    next(s_ptr)
    return zip(f_ptr, s_ptr)


def generate_session_from_event(events, max_ts_delta=300):
    """Generate sessions from events."""
    events_by_timestamp = sorted(events, key=lambda event: (event.transientId, event.ts))
    guard_event = Event(
        ts=None, persistentId=None, transientId=None, device_type=None, url=None
    )
    sessions = []

    session = Session(
        transientId=events_by_timestamp[0].transientId,
        persistentId=events_by_timestamp[0].persistentId,
        device_type=events_by_timestamp[0].device_type,
        events=[]
    )
    events_count = 0

    for event, next_event in consecutive_pairs(events_by_timestamp + [guard_event]):
        session.events.append(event)
        if event.transientId != next_event.transientId or (next_event.ts - event.ts).seconds > max_ts_delta:
            sessions.append(session)
            events_count += len(session.events)
            session = Session(
                transientId=next_event.transientId,
                persistentId=next_event.persistentId,
                device_type=next_event.device_type,
                events=[]
            )

    assert len(events_by_timestamp) == events_count
    return sessions


def get_session_duration(user_session):
    """Get session duration."""
    return user_session.events[-1].ts - user_session.events[0].ts


def get_time_by_device(user_sessions):
    """Get time spent on device."""
    time_by_device = defaultdict(timedelta)

    for session in user_sessions:
        time_by_device[session.transientId] += get_session_duration(session)

    return time_by_device


def generate_stats(all_activities, **kwargs):
    """Generate statistics per user (persistentId) activities."""
    result = dict()

    user_sessions = generate_session_from_event(all_activities, **kwargs)

    def grouper(session):
        return session.persistentId or session.transientId

    for persistent_id, session_list in (itertools.groupby(sorted(user_sessions, key=grouper), key=grouper)):
        session_list = list(session_list)
        session_durations = get_time_by_device(session_list)
        user_events_by_timestamp = sorted(
            itertools.chain.from_iterable([session.events for session in session_list]),
            key=lambda event: event.ts
        )

        if persistent_id not in result:
            result[persistent_id] = {
                "transient_ids": {},
                "devices_count": 0,
                "first_device": first_device_in_session(user_events_by_timestamp),
                "time_to_purchase": time_to_purchase(user_events_by_timestamp),
            }

        for transient_id, duration in session_durations.items():
            user_sessions = sorted(
                [session for session in session_list if session.transientId == transient_id],
                key=lambda session: session.events[0].ts
            )
            result[persistent_id]["transient_ids"][transient_id] = {
                "sessions_duration": duration,
                "sessions_count": len(user_sessions),
                "purchase_session": user_sessions[-1],
                "sessions": user_sessions
            }
            result[persistent_id]["devices_count"] += 1
    return result


def draw_referenced_subgraph(persistent_id, graph):
    drawing.draw(
        title=f"{persistent_id} path to purchase",
        scatters=list(
            drawing.edge_scatters_by_label(
                graph,
                opacity={"visited": 0.35, "purchase_path": 0.4},
                widths={"links_to": 0.2, "visited": 3, "purchase_path": 3},
                colors={"links_to": "grey", "purchase_path": "red"},
                dashes={"links_to": "dot"}
            )
        ) + list(
            drawing.scatters_by_label(
                graph, attrs_to_skip=["pos", "size"],
                sizes={
                    "event": 9,
                    "persistentId": 20,
                    "thank-you-page": 25,
                    "website": 25,
                    "session": 15,
                },
                colors={
                    "event": 'rgb(153,112,171)',
                    "session": 'rgb(116,173,209)',
                    "thank-you-page": 'orange',
                    "website": 'rgb(90,174,97)',
                    "transientId": 'rgb(158,1,66)',
                    "persistentId": 'rgb(213,62,79)'
                }
            )
        ),
    )


def compute_subgraph_pos(query_results, thank_you_page):
    """Given query results compute subgraph positions."""
    for persistent_id, raw_graph in _build_networkx_graph_single(
        query_results=query_results,
        thank_you_page=thank_you_page,
        max_ts_delta=300
    ):

        raw_graph.nodes[thank_you_page]["label"] = "thank-you-page"

        graph_with_pos_computed = drawing.layout(raw_graph, _custom_layout)

        yield persistent_id, graph_with_pos_computed


def custom_plots(data):
    """Build list of custom plot figures."""
    return [
        bar_plots.make_bars(
            {
                k[:5]: v["time_to_purchase"].total_seconds() / (3600 * 24)
                for k, v in data.items()
            },
            title="User's time to purchase",
            x_title="Persistent IDs",
            y_title="Days to purchase",
            lazy=True
        ),
        _show_session_stats(data, title="Per device session statistics"),
        _show_most_common_visited_webpages(data, title="Most common visited subpages before purchase", count=10),
    ]


# ===========================
# Everything below was added to introspect the query results via visualisations


def _show_session_stats(data, title):
    def sunburst_data(data):
        total_sum = sum(
            values["sessions_count"]
            for _, v in data.items()
            for values in v["transient_ids"].values()
        )
        yield "", "Users", 1.5 * total_sum, "white", ""

        for i, (persistentId, v) in enumerate(data.items(), 1):
            yield (
                "Users",
                persistentId[:5],
                sum(values["sessions_count"] for values in v["transient_ids"].values()),
                i,
                (
                    f"<br>persistentId: {persistentId} </br>"
                    f"devices count: {len(v['transient_ids'])}"
                )
            )
            for transientId, values in v["transient_ids"].items():
                yield (
                    persistentId[:5],
                    transientId[:5],
                    values["sessions_count"],
                    i,
                    (
                        f"<br>transientId: {transientId}"
                        f"<br>session count: {values['sessions_count']}"
                        f"<br>total session duration: {values['sessions_duration']}"
                    )
                )
                for session in values["sessions"]:
                    yield (
                        transientId[:5],
                        session.events[0].ts,
                        1,
                        i,
                        (
                            f"<br>session start: {session.events[0].ts}"
                            f"<br>session end: {session.events[-1].ts}"
                            f"<br>session duration: {session.events[-1].ts - session.events[0].ts}"
                        )
                    )
        # aka legend
        yield "Users", "User ids", total_sum / 2, "white", ""
        yield "User ids", "User devices", total_sum / 2, "white", ""
        yield "User devices", "User sessions", total_sum / 2, "white", ""

    parents, labels, values, colors, hovers = zip(*[r for r in list(sunburst_data(data))])

    fig = go.Figure(
        go.Sunburst(
            labels=labels,
            parents=parents,
            values=values,
            branchvalues="total",
            marker=dict(
                colors=colors,
                line=dict(width=0.5, color='DarkSlateGrey')
            ),
            hovertext=hovers,
            hoverinfo="text",
        ),
    )

    fig.update_layout(margin=dict(t=50, l=0, r=0, b=0), title=title)
    return fig


def _show_most_common_visited_webpages(data, title, count):
    def drop_qs(url):
        pos = url.find("?")
        if pos == -1:
            return url
        return url[0:pos]

    def compute_data(data):
        res = defaultdict(list)
        for v in data.values():
            for values in v["transient_ids"].values():
                for session in values["sessions"]:
                    for event in session.events:
                        res[drop_qs(event.url)].append(session.persistentId)
        return res

    def sunburst_data(data):
        total_sum = sum(len(v) for v in data.values())
        yield "", "websites", total_sum, ""
        for i, (website, persistents) in enumerate(data.items()):
            yield (
                "websites", f"Website {i}",
                len(persistents),
                f"<br>website: {website}"
                f"<br>users: {len(set(persistents))}"
                f"<br>events: {len(persistents)}"
            )
            for persistent, group in itertools.groupby(
                sorted(list(persistents)),
            ):
                group = list(group)
                yield (
                    f"Website {i}", persistent[:5],
                    len(group),
                    f"<br>persistentId: {persistent}"
                    f"<br>events: {len(group)}"
                )

    events_data = compute_data(data)
    most_common = dict(sorted(events_data.items(), key=lambda x: -len(x[1]))[:count])
    most_common_counts = {k: len(v) for k, v in most_common.items()}

    pie_chart = go.Pie(
        labels=list(most_common_counts.keys()),
        values=list(most_common_counts.values()),
        marker=dict(line=dict(color='DarkSlateGrey', width=0.5)),
        domain=dict(column=0)
    )

    parents, labels, values, hovers = zip(*[r for r in list(sunburst_data(most_common))])

    sunburst = go.Sunburst(
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        marker=dict(
            line=dict(width=0.5, color='DarkSlateGrey')
        ),
        hovertext=hovers,
        hoverinfo="text",
        domain=dict(column=1)
    )

    layout = go.Layout(
        grid=go.layout.Grid(columns=2, rows=1),
        margin=go.layout.Margin(t=50, l=0, r=0, b=0),
        title=title,
        legend_orientation="h"
    )

    return go.Figure([pie_chart, sunburst], layout)


def _build_networkx_graph_single(query_results, thank_you_page, **kwargs):
    def drop_qs(url):
        pos = url.find("?")
        if pos == -1:
            return url
        return url[0:pos]

    def transient_attrs(transient_id, transient_dict):
        return {
            "uid": transient_id,
            "sessions_count": len(transient_dict["sessions"]),
            "time_on_device": transient_dict["sessions_duration"]
        }

    def session_attrs(session):
        return hash((session.transientId, session.events[0])), {
            "duration": get_session_duration(session),
            "events": len(session.events)
        }

    def event_to_website(graph, event, event_label):
        website = drop_qs(event.url)
        graph.add_node(website, label="website", url=website)
        graph.add_node(hash(event), label=event_label, **event._asdict())
        graph.add_edge(website, hash(event), label="links_to")

    for persistent_id, result_dict in generate_stats(query_results, **kwargs).items():
        graph = nx.MultiGraph()
        graph.add_node(persistent_id, label="persistentId", pid=persistent_id)

        for transient_id, transient_dict in result_dict["transient_ids"].items():
            graph.add_node(transient_id, label="transientId", **transient_attrs(transient_id, transient_dict))
            graph.add_edge(persistent_id, transient_id, label="has_identity")

            for session in transient_dict["sessions"]:
                event_label = "event"
                if session == transient_dict["purchase_session"]:
                    event_edge_label = "purchase_path"
                else:
                    event_edge_label = "visited"

                session_id, session_node_attrs = session_attrs(session)
                # transient -> session
                graph.add_node(session_id, label="session", **session_node_attrs)
                graph.add_edge(session_id, transient_id, label="session")

                fst_event = session.events[0]
                # event -> website without query strings
                event_to_website(graph, fst_event, event_label)

                # session -> first session event
                graph.add_edge(session_id, hash(fst_event), label="session_start")

                for fst_event, snd_event in consecutive_pairs(session.events):
                    event_to_website(graph, fst_event, event_label)
                    event_to_website(graph, snd_event, event_label)
                    graph.add_edge(hash(fst_event), hash(snd_event), label=event_edge_label)
        graph.nodes[result_dict["first_device"]]["size"] = 15

        yield persistent_id, graph


def _custom_layout(graph):
    """Custom layout function."""
    def _transform_graph(graph):
        """
        Transform one graph into another for the purposes of better visualisation.

        We rebuild the graph in a tricky way to force the position computation algorithm
        to allign with the desired shape.
        """
        new_graph = nx.MultiGraph()

        for edge in graph.edges(data=True):
            fst, snd, params = edge
            label = params["label"]

            new_graph.add_node(fst, **graph.nodes[fst])
            new_graph.add_node(snd, **graph.nodes[snd])
            if label == "links_to":
                # website -> event
                # => event -> user_website -> website
                user_website = f"{fst}_{snd}"
                new_graph.add_node(user_website, label="user_website")
                new_graph.add_edge(snd, user_website, label="session_visit")
                new_graph.add_edge(user_website, fst, label="session_link")
            else:
                new_graph.add_edge(fst, snd, **params)

        return new_graph

    return nx.kamada_kawai_layout(_transform_graph(graph))
