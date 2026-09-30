from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "store_id").orderBy(F.col("amount").desc())
w_ordered = Window.partitionBy("channel", "customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("discount").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

scored = (
    df_orders
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.last("channel", ignorenulls=True).over(w_ordered).alias("last_known_channel_2"),
        F.last("amount", ignorenulls=True).over(w_recent).alias("last_known_amount"),
        F.rank().over(w5).alias("discount_rank"),
    )
)

result = (
    scored
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "last_known_channel",
        "amount_percent_rank",
        "last_known_channel_2",
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("discount").desc_nulls_first(), "order_date")
)
