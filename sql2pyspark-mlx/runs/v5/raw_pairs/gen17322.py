from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter((F.col("o.amount") < 940) | F.col("o.store_id").isNull())
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.discount"),
    )
)
