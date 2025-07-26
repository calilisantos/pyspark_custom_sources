# Databricks notebook source
# MAGIC %md
# MAGIC ## **Dependencies**

# COMMAND ----------

import os
from pyspark.sql import Row, SparkSession, types as T # pyspark==4.0.0
from pyspark.sql.datasource import DataSource, DataSourceReader # pyarrow required, used: pyarrow==19.0.1
import requests # used: requests==2.32.4
# optional, for jupyter notebook: ipykernel==6.26.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Utils:**
# MAGIC [StackOverflow rules!!!](https://stackoverflow.com/questions/74105403/determine-if-code-is-running-on-databricks-or-ide-pycharm)

# COMMAND ----------

is_databricks_session = "DATABRICKS_RUNTIME_VERSION" in os.environ

def show_dataframe(df, is_databricks):
    if is_databricks:
        display(df)
    else:
        df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SparkSession**

# COMMAND ----------

if not is_databricks_session:
    spark = (
        SparkSession.builder
            .master("local[3]")
                .appName("pyspark_custom_datasource")
                    .getOrCreate()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## **ChuckNorrisDataSource statement**

# COMMAND ----------

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

spark.dataSource.register(ChuckNorrisDataSource)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Reading ChuckNorrisDataSource**

# COMMAND ----------

# Getting 5 random facts
show_dataframe(
    df=spark.read.format("chucknorris")
        .option("count", 5)
            .load(),
    is_databricks=is_databricks_session
)


# COMMAND ----------

show_dataframe(
    df=spark.read.format("chucknorris")
        .load(),
    is_databricks=is_databricks_session
)
