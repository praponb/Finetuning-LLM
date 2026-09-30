from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w3 = Window.partitionBy("city").orderBy("sqft", "id")

scored = (
    df_stores
    .filter(F.col("region") != "East")
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.percent_rank().over(w).alias("sqft_percent_rank"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.ntile(10).over(w3).alias("sqft_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "region",
        "sqft_percent_rank",
        "sqft_cume_dist",
        "sqft_bucket",
        F.when(F.col("region").rlike("^(Central|South)$"), "OTHER_ZONE").otherwise("CORE").alias("region_zone"),
    )
    .orderBy(F.col("sqft_bucket").desc())
)
