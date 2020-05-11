import networkx as nx
from gremlin_python.process.graph_traversal import in_, coalesce, constant, select
from gremlin_python.process.traversal import T, P, Column

from nepytune import drawing


def query_website_node(g, website_id):
    return g.V(website_id).valueMap(True).toList()[0]


def query_transient_nodes_for_website(g, website_id, limit=10000):
    return (g.V(website_id)
            .in_("visited")
            .limit(limit)
            .project("uid", "pid")
            .by("uid")
            .by(in_("has_identity").values("pid").fold())
            .group()
            .by(coalesce(select("pid").unfold(), constant("transient-nodes-connected-to-website")))
            .by(select("uid").dedup().limit(100).fold())
            .unfold()
            .project("persistent-node-id", "transient-nodes")
            .by(select(Column.keys))
            .by(select(Column.values))
            .where(select("transient-nodes").unfold().count().is_(P.gt(1)))
            ).toList()


def create_graph_for_website_and_transient_nodes(website_node, transient_nodes_for_website):
    website_id = website_node[T.id]

    graph = nx.Graph()
    graph.add_node(
        website_id,
        **{
            "id": website_id,
            "label": website_node[T.label],
            "title": website_node["title"][0],
            "url": website_node["url"][0]
        }
    )

    transient_nodes = []
    persistent_nodes = []

    for node in transient_nodes_for_website:
        if node["persistent-node-id"] != "transient-nodes-connected-to-website":
            pnode = node["persistent-node-id"]
            persistent_nodes.append(pnode)
            graph.add_node(
                pnode,
                id=pnode,
                label="persistentId"
            )

            for tnode in node["transient-nodes"]:
                graph.add_edge(
                    pnode,
                    tnode,
                    label="has_identity"
                )

        for tnode in node["transient-nodes"]:
            graph.add_node(
                tnode,
                id=tnode,
                label="transientId"
            )

            graph.add_edge(
                website_id,
                tnode,
                label="visited"
            )

            transient_nodes.append(tnode)
    return graph


def show(g, website_id):
    """Show users that visited website on more than one device."""

    transient_nodes_for_website = query_transient_nodes_for_website(g, website_id)
    website_node = query_website_node(g, website_id)

    raw_graph = create_graph_for_website_and_transient_nodes(website_node, transient_nodes_for_website)
    graph = drawing.spring_layout(raw_graph)

    drawing.draw(
        title="",
        scatters=[drawing.edges_scatter(graph)] + list(drawing.scatters_by_label(graph, attrs_to_skip=["pos"])),
    )