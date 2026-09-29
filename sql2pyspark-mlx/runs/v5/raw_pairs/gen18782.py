from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("days_since_order_date", "id")
w2 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("store_id").orderBy("days_since_order_date_bucket_minus_amount", "id")
w5 = Window.partitionBy("channel").orderBy("cumulative_shipping_fee", "id")
w6 = Window.partitionBy("customer_id").orderBy("cumulative_shipping_fee", "id")
w7 = Window.partitionBy("status", "store_id").orderBy(F.col("days_since_order_date_bucket_minus_amount").desc_nulls_first())
w8 = Window.partitionBy("store_id", "status").orderBy("days_since_order_date_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("channel").orderBy("days_since_order_date_bucket_minus_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w).alias("days_since_order_date_bucket"),
        F.sum("shipping_fee").over(w2).alias("cumulative_shipping_fee"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.method"))
)

valid = (
    with_latest
    .filter((F.col("order_date") >= F.lit("2023-09-04").cast("date")) & F.col("customer_id").isin([27, 38]))
    .withColumn("days_since_order_date_bucket_minus_amount", (F.col("days_since_order_date_bucket") - F.col("amount")))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

scored_orders = (
    deduped
    .filter(F.col("order_date") > "2023-08-10")
    .select(
        "*",
        F.ntile(5).over(w5).alias("cumulative_shipping_fee_bucket"),
        F.row_number().over(w6).alias("seq_no"),
        F.cume_dist().over(w7).alias("days_since_order_date_bucket_minus_amount_cume_dist"),
        F.last("days_since_order_date_bucket_minus_amount", ignorenulls=True).over(w8).alias("last_known_days_since_order_date_bucket_minus_amount"),
    )
)

with_last = (
    scored_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"))
)

result = (
    with_last
    .select(
        "id",
        "store_id",
        "channel",
        "discount",
        "days_since_order_date_bucket",
        "cumulative_shipping_fee",
        "amount",
        "latest_rank",
        "last_known_days_since_order_date_bucket_minus_amount",
        F.avg("days_since_order_date_bucket_minus_amount").over(w_running).alias("running_avg_days_since_order_date_bucket_minus_amount"),
        F.max("days_since_order_date_bucket_minus_amount").over(w_running).alias("running_max_days_since_order_date_bucket_minus_amount"),
        F.count("days_since_order_date_bucket_minus_amount").over(w_running).alias("running_count_days_since_order_date_bucket_minus_amount"),
    )
)
