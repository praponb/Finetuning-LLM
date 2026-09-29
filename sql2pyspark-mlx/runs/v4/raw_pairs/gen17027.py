from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "status").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

metrics = (
    df_orders
    .select(
        "*",
        F.avg("shipping_fee").over(w).alias("shipping_fee_avg_6"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.ntile(5).over(w4).alias("shipping_fee_bucket"),
        F.ntile(5).over(w5).alias("shipping_fee_bucket_2"),
    )
)

result = (
    metrics
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_avg_6",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("shipping_fee").desc_nulls_last())
)
