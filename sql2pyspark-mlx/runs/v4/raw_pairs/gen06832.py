from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id", "channel").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

metrics = (
    df_orders
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("discount").over(w2).alias("customer_id_sum_discount"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("shipping_fee_cume_dist"),
    )
)

result = (
    metrics
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "shipping_fee",
        "customer_id_sum_discount",
        "row_seq",
        "shipping_fee_cume_dist",
        F.col("avg_order_shipping_fee"),
    )
    .orderBy(F.col("store_id").desc())
)
