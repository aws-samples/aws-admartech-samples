import logging
import os
import math
import random
import time
import asyncio

from datetime import timedelta

from gremlin_python.process.graph_traversal import values, outE, inE
from gremlin_python.process.traversal import Column, Order
from aiogremlin import DriverRemoteConnection, Graph
from aiogremlin.exception import GremlinServerError

from nepytune.usecase import (
    get_sibling_attrs, brand_interaction_audience,
    get_all_transient_ids_in_household, undecided_user_audience_check,
    undecided_users_audience, get_activity_of_early_adopters
)

logger = logging.getLogger(__name__)

ARG_COLLECTION = 1000
COIN = 0.1


class QueryRunner:
    """Query runner."""

    def __init__(self, query, samples):
        self.args = []
        self.query = query
        self.samples = int(samples)
        self.succeded = 0
        self.failed = 0

    async def run(self, sample, pool):
        """Run query and return measure."""
        sample_no = sample + 1
        try:
            connection = pool.lock()
            g = Graph().traversal().withRemote(connection)
            args = self.get_args(sample)
            try:
                start = time.time()
                result = await self.query(g, **args).toList()
                end = time.time()
                _log_query_info(self.samples, sample_no, args, result)
                self.succeded += 1
                return (start, end, end - start)
            except GremlinServerError as e:
                logger.debug(f"Sample {sample_no} failed: {e.msg}")
                self.failed += 1
                return None
            finally:
                pool.unlock(connection)
        except ConnectionError as e:
            logger.debug(f"Sample {sample_no} failed: {e}")
            self.failed += 1
            return None


    async def initialize(self):
        pass

    def get_args(self, sample):
        """Get args for query function."""
        return self.args[sample % len(self.args)]


class SiblingsAttrsRunner(QueryRunner):
    def __init__(self, samples):
        super().__init__(query=get_sibling_attrs, samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)
            transient_ids = await get_household_members(g, ARG_COLLECTION)

            self.args = [
                {
                    "transient_id": transient_id
                } for transient_id in transient_ids
            ]


class BrandInteractionRunner(QueryRunner):
    def __init__(self, samples):
        super().__init__(query=brand_interaction_audience, samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)
            websites = await (
                g.V().hasLabel("website").coin(COIN).limit(ARG_COLLECTION).toList()
            )

            self.args = [
                {
                    "website_url": website
                } for website in websites
            ]


class AudienceCheck(QueryRunner):
    def __init__(self, samples):
        self.args = []
        super().__init__(query=undecided_user_audience_check, samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)

            data = await (
                g.V().hasLabel("transientId").coin(COIN).limit(ARG_COLLECTION)
                .group()
                .by()
                .by(
                    outE("visited").coin(COIN).inV().in_(
                        "links_to").out("links_to").coin(COIN)
                    .path()
                    .by(values("uid"))
                    .by(values("ts"))
                    .by(values("url"))
                    .by(values("url"))
                    .by(values("url"))
                ).select(Column.values).unfold()
            ).toList()

            self.args = [
                {
                    "transient_id": result[0],
                    "website_url": result[2],
                    "thank_you_page_url": result[4],
                    "since": result[1] - timedelta(days=random.randint(30, 60)),
                    "min_visited_count": random.randint(2, 5)
                } for result in data if result
            ]


class AudienceGeneration(QueryRunner):
    def __init__(self, samples):
        self.args = []
        super().__init__(query=undecided_users_audience, samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)

            most_visited_websites = await get_most_active_websites(g)
            data = await (
                g.V(most_visited_websites)
                .group()
                .by()
                .by(
                    inE().hasLabel("visited").coin(COIN).inV()
                    .in_("links_to").out("links_to").coin(COIN)
                    .path()
                    .by(values("url"))  # visited website
                    .by(values("ts"))  # timestamp
                    .by(values("url"))  # visited website
                    .by(values("url"))  # root website
                    .by(values("url").limit(1))  # thank you page
                ).select(Column.values).unfold()
            ).toList()

            self.args = [
                {
                    "website_url": result[0],
                    "thank_you_page_url": result[4],
                    "since": result[1] - timedelta(days=random.randint(30, 60)),
                    "min_visited_count": random.randint(2, 5)
                } for result in data
            ]


class EarlyAdopters(QueryRunner):
    def __init__(self, samples):
        super().__init__(
            query=get_activity_of_early_adopters,
            samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)
            most_visited_websites = await get_most_active_websites(g)

            self.args = [
                {
                    "thank_you_page_url": website
                } for website in most_visited_websites
            ]


class HouseholdDevices(QueryRunner):
    def __init__(self, samples):
        super().__init__(query=get_all_transient_ids_in_household,
                         samples=samples)

    async def initialize(self):
        connection = await init_neptune_connection()
        async with connection:
            g = Graph().traversal().withRemote(connection)
            household_members = await get_household_members(g, ARG_COLLECTION)

            self.args = [
                {
                    "transient_id": member
                } for member in household_members
            ]


async def get_household_members(g, limit, coin=COIN):
    """Return transient IDs which are memebers of identity group."""
    return await (
        g.V().hasLabel("identityGroup").out("member")
        .out("has_identity")
        .coin(coin).limit(limit).toList()
    )


async def init_neptune_connection():
    """Init Neptune connection."""
    endpoint = os.environ["NEPTUNE_CLUSTER_ENDPOINT"]
    port = os.getenv("NEPTUNE_CLUSTER_PORT", "8182")
    return await DriverRemoteConnection.open(f"ws://{endpoint}:{port}/gremlin", "g")


def _log_query_info(samples, sample_no, args, result):
    logger.debug(f"Sample {sample_no} args: {args}")
    if len(result) > 100:
        logger.debug("Truncating query result.")
        logger.debug(f"Sample {sample_no} result: {result[:100]}")
    else:
        logger.debug(f"Sample {sample_no} result: {result}")

    samples_checkpoint = math.ceil(samples*0.1)
    if sample_no % samples_checkpoint == 0:
        logger.info(f"Finished {sample_no} of {samples} samples.")


async def get_most_active_websites(g):
    """Return websites with most visits."""
    # Query for most visited websites is quite slow.
    # Thus visited websites are hardcoded.

    # most_visited_websites = await (
    #     g.V().hasLabel("website")
    #     .order().by(inE('visited').count(), Order.decr)
    #     .limit(1000).toList()
    # )

    most_visited_websites = [
        "8f6b27fe6f0dcdae",
        "a997482113271d8f/5758f309e11931ce",
        "b23e286d713f61fd/e0da2d3e2c6f610/16e720804d7385cb?aac4d7fceeea7dcb",
        "6e89cfa05ae05032",
        "7e89190c7bcf1be9/32345249f712667/26010d49384ca927/01fee084a3cb3563?1d5bfa3db363b460",
        "3cfce7aac081cf80/49d249c29289f7a5/5ea0237ac10c9de3?1911788a62d90dd4",
        "12a78ad541e95ae",
        "7e89190c7bcf1be9/32345249f712667/26010d49384ca927/01fee084a3cb3563?77af8f56d61f1f7",
        "ed95a9a5be30e4c8/5162fc6a223f248d",
        "b23e286d713f61fd/e0da2d3e2c6f610/16e720804d7385cb",
        "2e272bb1ae067296/49ffef01dbcd3442",
        "6ea77fc3ea42bd5b",
        "4c980617e02858a4",
        "b23e286d713f61fd/f9077d4b41c9e32e",
        "c3c6e6e856091767",
        "12a78ad541e95ae/7de2f069da3a3655",
        "7e89190c7bcf1be9/32345249f712667/26010d49384ca927/01fee084a3cb3563?b80a3fe036e3d80",
        "6ae12ea8ec730ba5/281bb5a0f4846ea7/802fc6a2d4f41295/34702b07a20db/8b84b6e138385d6",
        "8f6f3d03e10289c2",
        "ed95a9a5be30e4c8",
        "ed95a9a5be30e4c8/9c2692a00033d2ca",
        "afea1067d86a1c44/768ddae806aa91cc",
        "7875af5f916d165/2de17cd3dfa1bafb?28d8c9221be3456e",
        "1f8649a74c661bd4",
        "ed95a9a5be30e4c8/d400c9e183de73f3",
        "0d9afe7c94a6fcb8",
        "5f63cba1308ebad/16e720804d7385cb?5a4b1b396bf1130",
        "b23e286d713f61fd/e0da2d3e2c6f610/16e720804d7385cb?a2ae02cc94e330f2",
        "6cb909d81a2f5b20",
        "b23e286d713f61fd/e0da2d3e2c6f610/16e720804d7385cb?799961866adb8a72",
        "5f63cba1308ebad/16e720804d7385cb?282d33b7392ed0f3",
        "b23e286d713f61fd/16e720804d7385cb",
        "dcb69d5b9ce0d93",
        "9e82d69ba38ad61",
        "1f8649a74c661bd4/b3cf138ac65a87cd",
        "427e6f941738985a",
        "8f6b27fe6f0dcdae/77cc413057b22ef2",
        "7e89190c7bcf1be9",
        "7e89190c7bcf1be9/fb5a409aecff2de1/32c6ffef1a8068b2/01fee084a3cb3563?590f324987a908ac",
        "277503b36e998a2c",
        "5bb77e7558c09124",
        "b23e286d713f61fd/16e720804d7385cb?799961866adb8a72",
        "6eefbbf46b47c5e",
        "dc958d5abcb0c7f4",
        "fb3859d88debbc2f/10e22e5ca30919fd/bed4d82bfc7fb316/9fb2db33a1362553/af1bef8666741753",
        "54df72c060e95707/01fee084a3cb3563",
        "1f73e4b495d6947a",
        "fb3859d88debbc2f/10e22e5ca30919fd/bed4d82bfc7fb316/9fb2db33a1362553/af1bef8666741753/b8b68b641a5d7f18",
        "6ae12ea8ec730ba5/281bb5a0f4846ea7/253bf3e95bec331a/34702b07a20db/8b84b6e138385d6",
        "5f63cba1308ebad/16e720804d7385cb",
        "a4e358da594acc69/d5e31c7559f5aae",
        "6e89cfa05ae05032?7ded49ef5f6ae4b5",
        "307809459d18aac/05ec660c9d33a602/1c4578927f3f3711/2ba906928c030c0f",
        "427e6f941738985a/7de2f069da3a3655",
        "70fc5e1c206b990d",
        "40c40bf5f58729e9",
        "2f38166a9f476d14/2e1f4252a64ef39e?ffa3ebbd543f63a",
        "8f6b27fe6f0dcdae/7de2f069da3a3655",
        "530bd88a2a6056ba/753be5bb22047d7d/ac5dd08add7bd9b3",
        "7e89190c7bcf1be9/32345249f712667/26010d49384ca927/01fee084a3cb3563?2cb5075b4f4e88dd",
        "c415bc2d4909291c/ff90c3dd68949525",
        "88784b4873c7551d/a8c79e6cf0f93af?3fe03b55422683a",
        "ec9d0d6b37ae8d68/01fee084a3cb3563",
        "ec9d0d6b37ae8d68/01fee084a3cb3563/850b51f8595b735c/d1559ef785b761e1",
        "999fd0543f2499ba/05ec660c9d33a602/1c4578927f3f3711/2ba906928c030c0f",
        "cf17e071ca4a6d63/333314eda494a273/9683443388b62d72",
        "afea1067d86a1c44/8968eb8d56ea2005",
        "6865c9a20330e96e",
        "afea1067d86a1c44/f13f8d0b2be7d308",
        "5f63cba1308ebad/16e720804d7385cb?9b2c7d0cf9c19280",
        "a4e358da594acc69",
        "043f71e11bce6115",
        "2f38166a9f476d14/2e1f4252a64ef39e?23cb33cf67558126",
        "2972e09dd52b5c34/e0da2d3e2c6f610/16e720804d7385cb?aac4d7fceeea7dcb",
        "ed95a9a5be30e4c8/9c2692a00033d2ca/de6b0a4bdf4056d8",
        "ef5e1c317855b110/d22919653063ad0f",
        "db7d0a15587e37",
        "fe5809a4bf69b53b",
        "c94174b63350fd53/1e8deebfc8e36e85/b5509c3fb28c4e4f",
        "f9717a397d602927",
        "c415bc2d4909291c",
        "97c681e48c2bd244",
        "ed95a9a5be30e4c8/9c2692a00033d2ca/51faf05ad73be17c",
        "38111edd541b4aa0",
        "6eefbbf46b47c5e/7de2f069da3a3655",
        "6cb909d81a2f5b20/16e720804d7385cb?106cec9ffea2f2df",
        "968c8e4fbbb8b0ce",
        "8f6f3d03e10289c2/7de2f069da3a3655",
        "ed95a9a5be30e4c8/5162fc6a223f248d/4dab901f0f98436",
        "a16689098c57e580",
        "f745af148dbad70c/8b9644ee902b2351/01fee084a3cb3563/33dcc329910a2ce2",
        "cf17e071ca4a6d63",
        "ed95a9a5be30e4c8/9c2692a00033d2ca/4dab901f0f98436",
        "afea1067d86a1c44",
        "2972e09dd52b5c34/e0da2d3e2c6f610/16e720804d7385cb",
        "04285bbaac4dba06/01fee084a3cb3563/26db9e0e4002aab4",
        "9cafb5406de1df9e",
        "9b569b834ef0716c/16e720804d7385cb?c5a19578c7c7204c",
        "521fca29d4156a9d",
        "f8c1d22d2e8ba7c4",
    ]

    return most_visited_websites


def get_query_runner(query, samples):
    """Query runner factory."""
    if query == 'get_sibling_attrs':
        return SiblingsAttrsRunner(samples)
    elif query == 'brand_interaction_audience':
        return BrandInteractionRunner(samples)
    elif query == 'get_all_transient_ids_in_household':
        return HouseholdDevices(samples)
    elif query == "undecided_user_check":
        return AudienceCheck(samples)
    elif query == "undecided_user_audience":
        return AudienceGeneration(samples)
    elif query == "early_website_adopters":
        return EarlyAdopters(samples)
