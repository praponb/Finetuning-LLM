from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date").between("2023-11-23", "2024-05-02")).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.carrier").isNotNull())
    .select(F.col("s.id"), F.col("s.cost"), F.col("s.status"), F.col("s.shipped_date"))
)
