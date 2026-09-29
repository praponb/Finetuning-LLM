from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.dayofmonth(F.col("o.order_date")).alias("order_date_day"),
    )
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)
