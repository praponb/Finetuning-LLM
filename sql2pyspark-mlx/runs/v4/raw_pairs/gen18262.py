from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc_nulls_first(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

latest = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.when(F.col("status").rlike("^(PAUSED|ACTIVE)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
