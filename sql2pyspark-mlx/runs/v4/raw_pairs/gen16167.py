from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy("city", "region").orderBy(F.col("sqft").asc_nulls_last())
w5 = Window.partitionBy("region").orderBy(F.col("city_count_sqft").desc())
w6 = Window.orderBy(F.col("city_count_sqft").asc_nulls_first())
w7 = Window.partitionBy("city")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

kept_keys = (
    filtered
    .select("region", "id")
    .exceptAll(
        df_stores
        .filter(F.col("sqft") >= 8251)
        .select("region", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("sqft_rank", F.rank().over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("city_count_sqft", F.count("sqft").over(w3))
    .withColumn("region_count_sqft", F.count("sqft").over(w2))
)

eligible = (
    scored
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.customer_id").isNull()), "left_semi")
)

result = (
    eligible
    .withColumn("sqft_dense_rank", F.dense_rank().over(w4))
    .withColumn("city_count_sqft_cume_dist", F.cume_dist().over(w5))
    .withColumn("city_count_sqft_dense_rank", F.dense_rank().over(w6))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w4))
    .withColumn("city_min_sqft_share", F.min("sqft_share").over(w7))
)
