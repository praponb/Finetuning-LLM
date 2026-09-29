from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("channel_length_2").desc_nulls_last())
w2 = Window.partitionBy("store_id").orderBy(F.col("order_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("status")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-04-20").cast("date"))
    .withColumn("channel_status", F.concat("channel", F.lit(", "), "status"))
    .withColumn("channel_length", F.length("channel"))
)

remaining_keys = (
    valid
    .select("id")
    .subtract(
        df_orders
        .filter(F.col("store_id").isin([29, 31]))
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("order_date") >= F.lit("2023-06-12").cast("date"))
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "channel_status",
        "channel_length",
        F.length("channel").alias("channel_length_2"),
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
    )
)

filtered = (
    base
    .filter(F.col("order_date") >= F.lit("2023-07-09").cast("date"))
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "channel_status",
        "channel_length",
        "channel_length_2",
        F.date_trunc("WEEK", "order_date").alias("order_date_period"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

enriched = (
    filtered
    .withColumn("channel_length_2_percent_rank", F.percent_rank().over(w))
    .withColumn("channel_length_count_30d", F.count("channel_length").over(w2))
    .withColumn("status_min_amount", F.min("amount").over(w3))
)

result = (
    enriched
    .groupBy(
        "status",
        F.when(F.col("channel_length") >= 77, "PREMIUM").otherwise("STANDARD").alias("channel_length_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.percentile_approx("shipping_fee", 0.9).alias("shipping_fee_p90"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount")).otherwise(0)).alias("completed_amount"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("pending_count") > 1)
)
