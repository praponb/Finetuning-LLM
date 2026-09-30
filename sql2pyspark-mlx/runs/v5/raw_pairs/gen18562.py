from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w3 = Window.orderBy(F.col("running_sum_amount").asc_nulls_first())
w4 = Window.orderBy(F.col("running_max_amount").desc_nulls_first())

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-07-07").cast("date"))
    .select(
        "*",
        F.concat("channel", F.lit(", "), "status").alias("channel_status"),
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "discount",
        "shipping_fee",
        "channel_status",
        F.max("amount").over(w).alias("running_max_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.ntile(3).over(w2).alias("shipping_fee_bucket"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("status").isin(["COMPLETED", "SHIPPED"]))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.carrier") == "Flash"), "left_anti")
)

scored = (
    kept
    .filter(F.col("shipping_fee_bucket").isNotNull())
    .withColumn("running_sum_amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("running_max_amount_percent_rank", F.percent_rank().over(w4))
)

kept_orders = (
    scored
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status").isin(["LOST", "RETURNED"])), "left_semi")
)

result = (
    kept_orders
    .select(
        "id",
        "shipping_fee",
        "channel_status",
        "running_max_amount",
        "running_sum_amount_dense_rank",
        "running_max_amount_percent_rank",
    )
)
