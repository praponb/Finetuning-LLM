from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("order_date_dayofweek", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("store_id").orderBy("shipping_fee_minus_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("running_order_date_dayofweek", "id")
w7 = Window.partitionBy("status", "payment_status").orderBy("cumulative_shipping_fee", "id")
w8 = Window.partitionBy("payment_status").orderBy("shipping_fee", "id")

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-05-12")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "shipping_fee",
        F.dayofweek("order_date").alias("order_date_dayofweek"),
        (F.col("shipping_fee") - F.col("discount")).alias("shipping_fee_minus_discount"),
    )
)

metrics = (
    cleaned
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("running_order_date_dayofweek", F.sum("order_date_dayofweek").over(w2))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"))
)

with_windows = (
    latest_match
    .filter(F.col("shipping_fee_minus_discount") >= 68)
    .withColumn("last_known_shipping_fee_minus_discount", F.last("shipping_fee_minus_discount", ignorenulls=True).over(w5))
    .withColumn("next_order_date_dayofweek", F.lead("order_date_dayofweek", 1, 0).over(w_recent))
    .withColumn("next_running_order_date_dayofweek", F.lead("running_order_date_dayofweek", 1, 0).over(w7))
    .withColumn("next_shipping_fee_minus_discount", F.lead("shipping_fee_minus_discount", 1).over(w8))
)

result = (
    with_windows
    .withColumn("cumulative_shipping_fee_tier", F.when(F.col("cumulative_shipping_fee") >= 152, "HIGH").otherwise("LOW"))
    .withColumn("next_running_order_date_dayofweek_tier", F.when(F.col("next_running_order_date_dayofweek") >= 307, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status", "cumulative_shipping_fee_tier", "next_running_order_date_dayofweek_tier"], ["status", "cumulative_shipping_fee_tier"], ["status"]],
        "status",
        "cumulative_shipping_fee_tier",
        "next_running_order_date_dayofweek_tier",
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("payment_status") == "FAILED", F.col("cumulative_shipping_fee"))).alias("failed_total_cumulative_shipping_fee"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("running_order_date_dayofweek")).otherwise(0)).alias("pending_running_order_date_dayofweek"),
    )
    .orderBy(F.col("next_running_order_date_dayofweek_tier").desc_nulls_last())
)
