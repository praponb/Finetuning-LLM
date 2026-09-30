from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("last7_sum_sqft").desc_nulls_last(), "id")
w3 = Window.partitionBy("region", "city").orderBy("last7_sum_sqft", "id")

with_windows = (
    df_stores
    .select(
        "*",
        F.count("sqft").over(w).alias("last7_count_sqft"),
        F.max("sqft").over(w).alias("last7_max_sqft"),
        F.avg("sqft").over(w).alias("last7_avg_sqft"),
        F.sum("sqft").over(w).alias("last7_sum_sqft"),
    )
)

valid = (
    with_windows
    .filter(F.col("region").isNotNull())
    .withColumn("store_name_filled", F.coalesce("store_name", F.lit("Unknown")))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

metrics = (
    eligible
    .select(
        "*",
        F.lag("last7_avg_sqft").over(w3).alias("prev_last7_avg_sqft"),
        F.lead("last7_avg_sqft").over(w3).alias("next_last7_avg_sqft"),
        F.lag("sqft").over(w3).alias("prev_sqft"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

eligible_stores = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible_stores
    .groupBy("region")
    .pivot("city", ["Berlin", "New York", "Singapore"])
    .agg(F.sum("prev_sqft").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "region",
        F.col("Berlin_total").alias("berlin_total"),
        F.col("Berlin_cnt").alias("berlin_cnt"),
        F.col("New York_total").alias("new_york_total"),
        F.col("New York_cnt").alias("new_york_cnt"),
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_cnt").alias("singapore_cnt"),
    )
)
