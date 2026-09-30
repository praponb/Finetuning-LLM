from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter((F.col("s.status") == "LOST") | (F.col("o.amount") < 1420))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("store_id").desc())
)
