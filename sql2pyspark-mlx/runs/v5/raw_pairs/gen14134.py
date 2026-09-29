from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.orderBy(F.col("sqft").desc_nulls_last())

filtered = (
    df_stores
    .filter(F.col("region") == "West")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
        F.concat_ws("-", "city", "store_name").alias("city_store_name_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

result = (
    filtered
    .filter(F.col("city") == "Berlin")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.avg("sqft").over(w).alias("sqft_avg_2"),
        F.ntile(10).over(w2).alias("sqft_bucket"),
        F.ntile(5).over(w3).alias("sqft_bucket_2"),
        F.dense_rank().over(w4).alias("sqft_dense_rank"),
    )
)
