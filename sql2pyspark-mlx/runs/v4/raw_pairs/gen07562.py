from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

eligible = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") < F.col("s.order_date")), "left_anti")
)

result = (
    eligible
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("sum_order_amount"),
    )
    .orderBy(F.col("discount").desc_nulls_first())
)
