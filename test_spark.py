import logging

import random
import faker
import pytest
from pyspark.sql import SparkSession, DataFrame

FAKE = faker.Faker()
LOG = logging.getLogger(__name__)
ARTISTS_COUNT = 5


@pytest.fixture(scope="session")
def spark():
    LOG.info(SparkSession.getActiveSession())
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def inconsistend_data(spark):
    authors = spark.createDataFrame(
        enumerate(FAKE.name() for _ in range(ARTISTS_COUNT)),
        schema="author_id: int, name: string",
    )
    # should be small, so use list
    # NOTE: toLocalIterator
    author_ids = [i.author_id for i in authors.select("author_id").collect()]

    # NOTE: withColumn
    books = spark.createDataFrame(
        ((FAKE.catch_phrase(), 2020 + i, i) for i in author_ids),
        schema="title: string, year: int, author_id: int",
    )

    return authors.filter(~authors.author_id.isin(random.sample(author_ids, 2))), books


def test_books(spark, inconsistend_data):
    LOG.info("start test")
    authors, books = inconsistend_data

    authors.show()
    authors.printSchema()
    books.show()
    books.printSchema()
    assert not authors
