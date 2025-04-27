from pyspark.sql import SparkSession  # SparkContext
from pyspark import SparkContext, SparkConf
import pytest
import logging

LOG = logging.getLogger(__name__)

@pytest.fixture
def sc():
    # return SparkContext("local", "SKU")
    LOG.info(SparkSession.getActiveSession())
    return SparkSession.builder.appName("SparkSample").master("spark://127.0.0.1:7077").getOrCreate()
    # return SparkSession.builder.remote("sc://127.0.0.1:7077").getOrCreate()
    # return SparkSession.builder.master("spark://d91d232e5b93:7077").getOrCreate()

    conf = SparkConf().setAppName('hello').setMaster('spark://127.0.0.1:7077')
    # conf = SparkConf().setAppName('hello').setMaster('spark://d91d232e5b93:7077')
    return SparkContext(conf=conf)


def test_sales(sc):
    LOG.info("start test")
    assert True