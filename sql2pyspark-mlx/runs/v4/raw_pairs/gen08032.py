from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_last(), "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("city_lower", F.lower("city"))
    .withColumn("sqft_rounded", F.round("sqft", 1))
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
