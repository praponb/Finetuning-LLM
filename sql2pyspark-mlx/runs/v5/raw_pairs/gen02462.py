from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy("quantity", "id")
w5 = Window.partitionBy("city").orderBy("quantity", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("running_max_sqft").desc())

latest = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    latest
    .filter(F.lower(F.trim("store_name")) == "airport kiosk")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.max("sqft").over(w2).alias("running_max_sqft"),
        F.avg("sqft").over(w2).alias("running_avg_sqft"),
        F.count("sqft").over(w2).alias("running_count_sqft"),
        F.sum("sqft").over(w2).alias("running_sum_sqft"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.opened_date",
        "s.running_max_sqft",
        "s.running_count_sqft",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

metrics_stores = (
    with_last
    .select(
        "*",
        F.lag("running_count_sqft").over(w4).alias("prev_running_count_sqft"),
        F.lag("running_max_sqft").over(w4).alias("prev_running_max_sqft"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("running_count_sqft").over(w5).alias("running_count_sqft_avg_5_prior"),
    )
)

latest_match = (
    metrics_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.running_max_sqft",
        "s.running_count_sqft",
        "s.quantity",
        "s.prev_running_count_sqft",
        "s.running_count_sqft_avg_5_prior",
        F.col("c.shipping_fee"),
        F.col("c.channel").alias("order_channel"),
    )
)

ranked = (
    with_latest
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("running_count_sqft_avg_5_prior") >= 15, "PREMIUM").otherwise("STANDARD").alias("running_count_sqft_avg_5_prior_tier"),
    )
    .agg(
        F.round(F.avg("running_max_sqft"), 2).alias("avg_running_max_sqft"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.round(F.sum("running_count_sqft_avg_5_prior") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("running_count_sqft_avg_5_prior_per_bangkok"),
        F.min("running_max_sqft").alias("min_running_max_sqft"),
    )
    .orderBy("running_count_sqft_avg_5_prior_per_bangkok")
)
