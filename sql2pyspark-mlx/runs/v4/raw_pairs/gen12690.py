from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

ranked = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "row_num",
        F.when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").when(F.col("channel").rlike("^(mobile|store)$"), "CORE").otherwise("PRIORITY").alias("channel_zone"),
        F.col("min_order_shipping_fee"),
    )
    .orderBy("row_num", "id")
)
