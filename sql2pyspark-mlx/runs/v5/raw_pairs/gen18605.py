from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee") < 27)
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("s.sqft"),
        F.add_months(F.col("o.order_date"), 3).alias("order_date_plus_months"),
    )
)
