from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter((F.col("o.shipping_fee") >= 18) & F.col("o.customer_id").isin([23, 25]))
    .select(
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("o.amount"),
        F.col("o.status"),
    )
    .orderBy("status", "store_name")
)
