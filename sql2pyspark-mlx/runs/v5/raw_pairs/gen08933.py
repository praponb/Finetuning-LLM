from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("method").orderBy("discount", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-07-24 04:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.floor("amount").alias("amount_whole"),
    )
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        F.col("d.order_date"),
        F.col("d.shipping_fee"),
        F.col("d.discount"),
    )
)

latest_payments = (
    joined
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "method", "shipping_fee")
)

result = (
    latest_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "method", F.col("max_payment_amount"))
    .orderBy(F.col("order_id").asc_nulls_last())
)
