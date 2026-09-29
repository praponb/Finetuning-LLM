from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("status") == "SHIPPED").select("id")

result = (
    df_shipments.alias("s")
    .join(excluded_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
    )
    .orderBy(F.col("order_id").desc())
)
