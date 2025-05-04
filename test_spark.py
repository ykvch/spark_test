import logging

import random
import faker
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

FAKE = faker.Faker()
LOG = logging.getLogger(__name__)
ARTISTS_COUNT = 5


@pytest.fixture(scope="session")
def spark():
    LOG.info(SparkSession.getActiveSession())
    return SparkSession.builder.getOrCreate()


# Generate 2 df in one fixture to keep their inconsistency consistent :)
@pytest.fixture
def inconsistend_data(spark) -> (DataFrame, DataFrame):
    authors = spark.createDataFrame(
        enumerate(FAKE.name() for _ in range(ARTISTS_COUNT)),
        schema="author_id: int, name: string",
    )
    # should be small, so use list
    # NOTE: toLocalIterator
    author_ids = [i.author_id for i in authors.select("author_id").collect()]

    # NOTE: withColumn
    books = spark.createDataFrame(
        (
            (FAKE.catch_phrase(), 2020 + i, random.randint(-5, 10), i)
            for i in author_ids
        ),
        schema="title: string, year: int, sold: int, author_id: int",
    )

    return authors.filter(~authors.author_id.isin(random.sample(author_ids, 2))), books


@pytest.mark.skip
def test_books(spark, inconsistend_data):
    LOG.info("start test")
    authors, books = inconsistend_data

    authors.show()
    authors.printSchema()
    books.show()
    books.printSchema()
    assert authors  # random fake assert


def test_missing_author(inconsistend_data):
    authors: DataFrame = inconsistend_data[0]  # assign separately due to...
    books: DataFrame = inconsistend_data[1]  # ...jedi typing IDE glitch

    j = books.join(authors, on="author_id", how="left")
    j.show()
    # j.author_id <-> col("author_id").isNull()
    assert not j.filter(j.author_id.isNull()), "Books missing author ID"


def test_missing_via_antijoin(inconsistend_data):
    """Spark has anti-join o_O"""
    authors: DataFrame = inconsistend_data[0]
    books: DataFrame = inconsistend_data[1]

    j = books.join(authors, on="author_id", how="leftanti")
    authors.show()
    j.show()
    assert not j


def test_suspicious_returns(inconsistend_data):
    books: DataFrame = inconsistend_data[1]
    suspicious_returns = books.filter(books.sold < 0)
    suspicious_returns.show()
    assert not suspicious_returns
