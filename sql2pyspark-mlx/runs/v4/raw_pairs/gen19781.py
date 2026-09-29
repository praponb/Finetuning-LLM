from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("order_date") < "2025-03-01").select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("p.order_id").isin([12, 31]))
    .select(F.col("p.id"), F.col("p.method"), F.col("p.status"))
)
