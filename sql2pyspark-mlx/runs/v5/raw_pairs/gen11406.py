from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_running = Window.partitionBy("currency", "plan")
w_recent = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").desc_nulls_last())
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("currency_max_val").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

metrics = (
    flattened
    .filter(F.col("val").isNotNull())
    .select(
        "*",
        F.max("val").over(w).alias("currency_max_val"),
        F.sum("val").over(w_running).alias("currency_sum_val"),
        F.min("val").over(w_recent).alias("plan_min_val"),
    )
)

filtered = (
    metrics
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "currency_max_val",
        "currency_sum_val",
        "plan_min_val",
        F.length("currency").alias("currency_length"),
        F.upper("currency").alias("currency_upper"),
    )
)

valid = (
    filtered
    .filter(F.col("plan_min_val") >= 67)
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("val_plus_plan_min_val", (F.col("val") + F.col("plan_min_val")))
)

with_windows = (
    valid
    .select(
        "plan",
        "currency_max_val",
        "currency_sum_val",
        "currency_length",
        "clean_plan",
        "val_plus_plan_min_val",
        F.count("currency_sum_val").over(w_recent).alias("plan_count_currency_sum_val"),
        F.stddev_samp("val").over(w_running).alias("currency_stddev_samp_val"),
        F.cume_dist().over(w4).alias("plan_min_val_cume_dist"),
        F.max("val").over(w).alias("currency_max_val_2"),
        F.dense_rank().over(w5).alias("currency_max_val_dense_rank"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("currency_max_val") >= 61, "HIGH").otherwise("LOW").alias("currency_max_val_tier"),
        F.when(F.col("plan_count_currency_sum_val") >= 14, "HIGH").otherwise("LOW").alias("plan_count_currency_sum_val_tier"),
        "plan",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("pro_events") >= 1)
)

result = (
    daily
    .select(
        "currency_max_val_tier",
        "plan_count_currency_sum_val_tier",
        "plan",
        "pro_events",
        "num_plans",
    )
    .orderBy(F.col("num_plans").desc_nulls_last())
)
