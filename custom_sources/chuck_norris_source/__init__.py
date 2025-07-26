from pyspark.sql import Row, SparkSession, types as T
from pyspark.sql.datasource import DataSource, DataSourceReader # pyarrow required, used: pyarrow==19.0.1
import requests


class ChuckNorrisDataSource(DataSource):
    """
    A DataSource for reading facts (jokes) from the Chuck Norris API.

    Name: `chucknorris`

    Schema: `id string, fact string, category string`

    Options:
        count: int 
            The number of facts to be returned. Default is 1

    Examples
    --------
    Register the data source:

    >>> from your_module import ChuckNorrisDataSource
    >>> spark.dataSource.register(ChuckNorrisDataSource)

    Load a few facts (you can specify how many):


    >>> spark.read.format("chucknorris").option("count", 5).load().show()
    +--------------------+--------------------+-----------+
    |                 id |                fact|   category|
    +--------------------+--------------------+-----------+
    | ykC28btrRCm4Vqev...| Chuck Norris can...|     animal|
    |        ...         |               ...  |       ... |
    +--------------------+--------------------+-----------+
    """

    @classmethod
    def name(cls):
        return "chucknorris"

    def schema(self):
        return T.StructType([
            T.StructField(name="id", dataType=T.StringType(), nullable=False),
            T.StructField(name="fact", dataType=T.StringType(), nullable=False),
            T.StructField(name="category", dataType=T.StringType(), nullable=False)
        ])

    def reader(self, schema):
        return ChuckNorrisReader(self.options)


class ChuckNorrisReader(DataSourceReader):
    def __init__(self, options):
        self.count = int(options.get("count", 1))  # Default: 1 fact

    def read(self, partition):
        url = "https://api.chucknorris.io/jokes/random"
        for _ in range(self.count):
            response = requests.get(url)
            response.raise_for_status()
            fact_data = response.json()
            yield Row(
                id=fact_data.get("id"),
                fact=fact_data.get("value"),
                category=(
                    fact_data.get("categories")[0]
                    if fact_data.get("categories")
                    else "uncategorized"
                )
            )

