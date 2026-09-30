from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "attr_value"),
    )
)

lines = (
    flattened
    .select("id", "k", "attr_value", F.explode_outer("coupon_codes").alias("value"))
)

cleaned = (
    lines
    .filter(~F.col("value").eqNullSafe(F.lit("VIP")))
    .select(
        "*",
        F.length("value").alias("value_length"),
        F.coalesce("value", F.lit("OTHER")).alias("value_clean"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("value") == "WELCOME5")
    .select(
        "id",
        "k",
        "attr_value",
        "value_length",
        "value_clean",
        F.stddev_samp("value_length").over(w).alias("value_stddev_samp_value_length"),
    )
)

stats = (
    with_windows
    .withColumn("value_stddev_samp_value_length_tier", F.when(F.col("value_stddev_samp_value_length") >= 60, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["k", "value_stddev_samp_value_length_tier"], ["k"], []],
        "k",
        "value_stddev_samp_value_length_tier",
    )
    .agg(
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.sum(F.when(F.col("k") == "gift", F.col("value_length"))).alias("gift_total_value_length"),
        F.round(F.avg("value_length"), 2).alias("avg_value_length"),
        F.grouping("k").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "k",
        "value_stddev_samp_value_length_tier",
        "device_count",
        "avg_value_length",
        "grouping_id",
    )
    .orderBy(F.col("avg_value_length").desc())
)
