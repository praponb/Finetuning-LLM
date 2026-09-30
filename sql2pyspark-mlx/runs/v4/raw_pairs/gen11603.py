from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("o.store_id") == 11) & (F.col("s.status") == "IN_TRANSIT"))
    .select(
        F.col("s.cost"),
        F.col("o.amount"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.upper(F.col("o.status")).alias("status_upper"),
    )
    .orderBy("cost", "country")
)
