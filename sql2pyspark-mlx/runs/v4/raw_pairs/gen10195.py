from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

enriched = (
    df_stores
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
    .withColumn("sqft_bucket", F.ntile(10).over(w4))
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.discount") <= 0.34), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

totals = (
    with_last
    .groupBy(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.min("row_seq").alias("min_row_seq"),
        F.sum("sqft_bucket").alias("total_sqft_bucket"),
        F.round(F.sum("sqft_bucket") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("sqft_bucket_per_west"),
    )
)

result = (
    totals
    .select(
        "week_start",
        "num_cities",
        "east_events",
        "total_sqft_bucket",
        "sqft_bucket_per_west",
    )
    .orderBy(F.col("east_events").desc_nulls_last())
)
