from datetime import datetime, timedelta


from gremlin_python.process.graph_traversal import in_, inE, select, out, values, has
from gremlin_python.process.traversal import P, Column


def get_all_devices_from_website_visitors(g, website_id, limit=100):
    """Get all transient ids (including siblings), that visited given page."""

    return (
        g.V(website_id)
            .project("transient_ids_no_persistent", "transient_ids_with_siblings")
                .by(
                    in_("visited").limit(limit).fold()
                )
                .by(
                    in_("visited").in_("has_identity").dedup().out("has_identity").limit(limit).fold()
                )
            .select(Column.values).unfold().unfold().dedup()
    )


def query_users_intersted_in_content(g, iab_codes, limit=10000):
    """Get users (persistent identities) that interacted with websites with given iab codes."""

    return (
        g.V()
            .hasLabel("persistentId")
            .coin(0.8)
            .limit(limit)
            .where(out("has_identity")
                   .out("visited")
                   .in_("links_to")
                   .has("categoryCode", P.within(iab_codes))
            )
            .project("persistent_id", "attributes", "ip_location")
                .by(values("pid"))
                .by(
                    out("has_identity").valueMap("browser", "email", "uid").unfold()
                    .group()
                        .by(Column.keys)
                        .by(select(Column.values).unfold().dedup().fold())
                )
                .by(out("has_identity").out("uses").dedup().valueMap().fold())
        )


def query_users_active_in_given_date_intervals(g, dt_conditions, limit=300):
    """Get users (persistent identities) that interacted with website in given date interval."""

    return (
        g.V().hasLabel("persistentId")
            .coin(0.5)
            .limit(limit)
            .where(
                out("has_identity").outE("visited").or_(
                    *dt_conditions
                )
            )
            .project("persistent_id", "attributes", "ip_location")
                .by(values("pid"))
                .by(
                    out("has_identity").valueMap("browser", "email", "uid").unfold()
                        .group()
                        .by(Column.keys)
                        .by(select(Column.values).unfold().dedup().fold())
                )
                .by(out("has_identity").out("uses").dedup().valueMap().fold())
    )


def query_users_active_in_n_days(g, n=30, today=datetime(2016, 6, 22, 23, 59), limit=1000):
    """Get users that were active in last 30 days."""

    dt_condition = [
        has("ts", P.gt(today - timedelta(days=n)))
    ]
    return query_users_active_in_given_date_intervals(g, dt_condition, limit)