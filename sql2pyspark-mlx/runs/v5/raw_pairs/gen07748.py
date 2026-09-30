from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("status") == "RETURNED").select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("s.shipped_date").between("2023-08-26", "2025-10-11"))
    .select(F.col("s.id"), F.col("s.order_id"))
)
