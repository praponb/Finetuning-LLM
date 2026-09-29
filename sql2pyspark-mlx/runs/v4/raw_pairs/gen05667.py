from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("amount", "id")
w4 = Window.partitionBy("store_id", "status").orderBy("discount", "id")

scored = (
    df_orders
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("amount_bucket", F.ntile(5).over(w3))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

filtered = (
    latest
    .filter((F.col("order_date") > F.lit("2024-04-30").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "event_seq",
        "seq_no",
        "amount_bucket",
        "row_num",
        F.concat_ws("-", "status", "channel").alias("status_channel_key"),
        F.greatest("amount_bucket", "discount").alias("greatest_amount_bucket_discount"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("greatest_amount_bucket_discount") >= 86, "PREMIUM").otherwise("STANDARD").alias("greatest_amount_bucket_discount_tier"),
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.count("*").alias("row_count"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("event_seq_per_pending"),
    )
    .filter(F.col("row_count") < 1)
)
