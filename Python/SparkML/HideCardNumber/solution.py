import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import PandasUDFType


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def card_number_mask(s: pd.Series) -> pd.Series:
    return s.str.slice_replace(start=4, stop=12, repl='XXXXXXXX')


if __name__ == "__main__":
    spark = SparkSession.builder.appName('PySparkUDF').getOrCreate()
    df = spark.createDataFrame([(1, "4042654376478743"), (2, "4042652276478747")], ["id", "card_number"])
    df.show()
    dfr = df.withColumn("hidden", card_number_mask("card_number"))
    dfr.show(truncate=False)
