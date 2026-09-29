from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("shipping_fee", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("store_id", "channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

with_windows = (
    df_orders
    .withColumn("discount_avg_2_prior", F.avg("discount").over(w))
    .withColumn("first_known_shipping_fee", F.first("shipping_fee", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "discount",
        "discount_avg_2_prior",
        "first_known_shipping_fee",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
