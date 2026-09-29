from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_first())

base = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2024-04-04")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

result = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
