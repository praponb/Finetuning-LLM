from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").asc_nulls_last())
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "customer_id")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_filled").desc(), "id")
w5 = Window.partitionBy("customer_id", "plan").orderBy("running_monthly_fee_filled", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
w7 = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w8 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_filled").asc_nulls_first())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2024-03-01").cast("date")) & F.col("customer_id").isNotNull() & F.col("plan").isin(["basic", "standard", "enterprise"]))
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

with_windows = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.sum("monthly_fee_filled").over(w2).alias("running_monthly_fee_filled"),
        F.min("monthly_fee").over(w3).alias("plan_min_monthly_fee"),
    )
)

enriched = (
    with_windows
    .withColumn("monthly_fee_filled_bucket", F.ntile(5).over(w4))
    .withColumn("monthly_fee_rank_avg_6", F.avg("monthly_fee_rank").over(w5))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "monthly_fee_filled",
        "status_prefix",
        "running_monthly_fee_filled",
        "monthly_fee_rank_avg_6",
        F.col("avg_order_shipping_fee"),
    )
)
