from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.store_id").isin([6, 26, 31]))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.coalesce(F.col("p.amount"), F.lit(0)).alias("amount_filled"),
    )
    .orderBy("channel")
)
