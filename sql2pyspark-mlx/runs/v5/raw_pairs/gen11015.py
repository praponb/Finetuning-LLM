from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.status") != "SUCCESS")
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.ceil(F.col("s.sqft")).alias("sqft_whole"),
    )
    .orderBy(F.col("sqft").desc())
)
