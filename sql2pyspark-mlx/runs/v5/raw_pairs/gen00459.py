from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("element")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("customer_id", "attr_key")
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("attr_key").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "attr_value",
        F.explode("coupon_codes").alias("element"),
    )
)

filtered = (
    flattened_carts
    .filter(F.col("attr_key").isin(["gift", "device"]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_key",
        "attr_value",
        "element",
        F.upper("attr_key").alias("attr_key_upper"),
        F.concat_ws(",", "attr_key", "element").alias("attr_key_element_key"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

valid = (
    filtered
    .filter(~F.col("attr_key").eqNullSafe(F.lit("gift")))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("element_length", F.length("element"))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("NONE")))
)

metrics = (
    valid
    .select(
        "*",
        F.min("created_at_hour").over(w_recent).alias("element_min_created_at_hour"),
        F.min("created_at_hour").over(w_recent).alias("element_min_created_at_hour_2"),
    )
)

scored = (
    metrics
    .withColumn("created_at_hour_count_30m", F.count("created_at_hour").over(w2))
    .withColumn("customer_id_stddev_samp_element_length", F.stddev_samp("element_length").over(w3))
    .withColumn("attr_key_avg_element_min_created_at_hour_2", F.avg("element_min_created_at_hour_2").over(w4))
    .withColumn("element_min_created_at_hour_2_max_15m", F.max("element_min_created_at_hour_2").over(w5))
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"), F.col("b.segment"), F.col("b.name"))
)

result = (
    joined
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "element"], ["country"]],
        F.col("p.country").alias("country"),
        F.col("src.element").alias("element"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.element_min_created_at_hour_2_max_15m")), 2).alias("avg_element_min_created_at_hour_2_max_15m"),
        F.round(F.avg(F.col("src.attr_key_avg_element_min_created_at_hour_2")), 2).alias("avg_attr_key_avg_element_min_created_at_hour_2"),
    )
    .orderBy(F.col("avg_element_min_created_at_hour_2_max_15m").desc_nulls_last())
)
