from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("o.amount") <= 1000) & F.col("o.customer_id").isin([33, 37]))
    .select(
        F.col("p.amount"),
        F.col("o.order_date"),
        F.col("s.store_name"),
        F.initcap(F.trim(F.col("p.status"))).alias("status_title"),
    )
)
