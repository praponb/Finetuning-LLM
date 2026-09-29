from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("status") != "RETURNED").select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.hour(F.col("p.paid_at")).between(6, 11))
    .select(F.col("p.id"), F.col("p.order_id"))
    .orderBy(F.col("order_id").desc())
)
