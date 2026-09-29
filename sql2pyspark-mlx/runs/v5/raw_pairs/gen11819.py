from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.delivered_date").between("2024-12-01", "2025-10-27") & F.col("o.amount").isNotNull())
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.initcap(F.trim(F.col("s.status"))).alias("status_title"),
    )
)
