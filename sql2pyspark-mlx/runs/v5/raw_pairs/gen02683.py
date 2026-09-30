from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")
w2 = Window.partitionBy("key").orderBy(F.col("key_length").desc_nulls_first())
w3 = Window.partitionBy("key").orderBy("key_length")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

flattened_carts = (
    flattened
    .select("id", "key", "attr_value", F.explode_outer("coupon_codes").alias("element"))
)

valid = (
    flattened_carts
    .filter(F.col("element").isNotNull() & (F.col("element").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "*",
        F.length("key").alias("key_length"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("key_length") > 62)
    .select(
        "key",
        "attr_value",
        "key_length",
        "key_filled",
        F.sum("key_length").over(w).alias("element_sum_key_length"),
        F.cume_dist().over(w2).alias("key_length_cume_dist"),
        F.rank().over(w3).alias("key_length_rank"),
    )
)

filtered = (
    metrics
    .filter((F.col("element_sum_key_length") <= 282) & F.col("key").isNotNull())
    .withColumn("key_length_adjusted", F.round((F.col("key_length") * 0.9), 2))
)

result = (
    filtered
    .cube(
        "key",
        F.when(F.col("key_length") >= 82, "LARGE").otherwise("SMALL").alias("key_length_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("key") == "gift", F.col("key_length_adjusted")).otherwise(0)).alias("gift_key_length_adjusted"),
        F.round(F.avg("key_length"), 2).alias("avg_key_length"),
        F.sum(F.when(F.col("key") == "device", F.col("element_sum_key_length"))).alias("device_total_element_sum_key_length"),
        F.count("*").alias("row_count_2"),
    )
    .filter(F.col("device_total_element_sum_key_length") < 1000000)
    .orderBy("row_count_2")
)
