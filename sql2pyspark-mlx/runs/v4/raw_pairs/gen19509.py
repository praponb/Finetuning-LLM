from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date").between("2023-05-02", "2024-06-01")).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("s.carrier"),
    )
)
