from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(~((F.col("o.store_id") == 10) & F.col("o.order_date").between("2024-09-01", "2025-05-23")))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.initcap(F.trim(F.col("s.status"))).alias("status_title"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
