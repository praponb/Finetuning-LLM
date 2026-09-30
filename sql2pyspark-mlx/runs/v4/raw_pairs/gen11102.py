from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("segment").orderBy(F.col("qty").desc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened_carts = (
    flattened
    .select(
        "id",
        "customer_id",
        "qty",
        "price",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

joined = (
    flattened_carts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.segment"))
)

cleaned = (
    joined
    .filter(F.col("value").isNotNull() & F.col("value").isin(["BUNDLE", "VIP", "SPRING10"]))
    .select(
        "customer_id",
        "qty",
        "price",
        "segment",
        F.length("segment").alias("segment_length"),
        F.coalesce("value", F.lit("OTHER")).alias("resolved_value"),
    )
)

filtered = (
    cleaned
    .filter(F.col("customer_id").isin([3, 30]))
    .select(
        "customer_id",
        "qty",
        "price",
        "segment",
        "segment_length",
        "resolved_value",
        F.trim("segment").alias("clean_segment"),
    )
)

enriched = (
    filtered
    .withColumn("customer_id_sum_segment_length", F.sum("segment_length").over(w))
    .withColumn("qty_rank", F.rank().over(w2))
    .withColumn("qty_cume_dist", F.cume_dist().over(w3))
)

valid = (
    enriched
    .filter(F.col("customer_id_sum_segment_length") >= 153)
    .select("*", F.lower("segment").alias("segment_lower"))
)

result = (
    valid
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("segment") == "SMB", F.col("segment_length"))).alias("smb_max_segment_length"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .orderBy(F.col("corporate_count").desc())
)
