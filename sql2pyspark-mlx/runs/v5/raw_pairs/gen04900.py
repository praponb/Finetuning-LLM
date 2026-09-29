from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("region")
w_part = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("city") != "New York"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.regexp_replace("store_name", " ", "-").alias("store_name_slug"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    ranked
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-06-29")))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "store_name_slug",
        F.concat("region", F.lit("/"), "city").alias("region_city"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

metrics = (
    filtered
    .filter(F.col("opened_date") >= "2025-02-22")
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("sqft_dense_rank_2", F.dense_rank().over(w_part))
)

result = (
    metrics
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "store_name_slug",
        "region_city",
        "resolved_region",
        "sqft_share",
        "sqft_dense_rank_2",
    )
)
