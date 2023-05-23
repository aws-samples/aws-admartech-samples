from datetime import datetime, timedelta

from gremlin_python.process.graph_traversal import select, has, unfold
from gremlin_python.process.traversal import P


def get_timerange_condition(g, start_hour=16, end_hour=18, limit=1000):
    dates = (
        g.E()
            .hasLabel("visited")
            .limit(limit)
            .values("ts")
            .fold()
            .as_("timestamps")
            .project("start", "end")
                .by(select("timestamps").unfold().min_())
                .by(select("timestamps").unfold().max_())
    ).next()

    start = dates["start"].replace(hour=start_hour, minute=0, second=0)
    end = dates["end"].replace(hour=start_hour, minute=0, second=0)
    
    toReturn = []
    
    for days in range((end - start).days):
        toReturn.append(
            has(
                'ts',
                P.between(
                    start + timedelta(days=days),
                    start + timedelta(days=days) + timedelta(hours=end_hour - start_hour)
                )
            )
            
        )

    return toReturn

#     [
#         has(
#             'ts',
#             P.between(
#                 start + timedelta(days=days),
#                 start + timedelta(days=days) + timedelta(hours=end_hour - start_hour)
#             )
#         )
#         for days in range((end - start).days)
#     ]


def get_user_device_statistics(g, dt_conditions, limit=10000):
    return (
        g.E().hasLabel("visited").or_(*dt_conditions)
        .limit(limit).outV().fold()
        .project("type", "device", "browser")
            .by(
                unfold().unfold().groupCount().by("type")
            )
            .by(
                unfold().unfold().groupCount().by("device")
            )
            .by(
                unfold().unfold().groupCount().by("browser")
            )
    )