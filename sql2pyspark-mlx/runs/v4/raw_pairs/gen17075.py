from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc_nulls_first())
w_grp = Window.partitionBy("status").orderBy("status_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("amount_percent_rank", "id")

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.order_date"), F.col("p.shipping_fee"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.sum("amount").over(w).alias("status_sum_amount"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
    )
)

metrics_payments = (
    metrics
    .withColumn("cumulative_shipping_fee", F.sum("shipping_fee").over(w_grp))
    .withColumn("amount_percent_rank_bucket", F.ntile(4).over(w_part))
)

result = (
    metrics_payments
    .withColumn("amount_percent_rank_tier", F.when(F.col("amount_percent_rank") >= 0.83, "HIGH").otherwise("LOW"))
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 41, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_percent_rank_tier", "shipping_fee_tier"], ["amount_percent_rank_tier"]],
        "amount_percent_rank_tier",
        "shipping_fee_tier",
    )
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.countDistinct("status").alias("distinct_status"),
        F.grouping("amount_percent_rank_tier").alias("gid"),
    )
    .orderBy("distinct_status")
)
