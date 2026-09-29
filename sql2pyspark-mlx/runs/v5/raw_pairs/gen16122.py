from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("customer_id") == 27).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.to_date(F.col("p.paid_at")) >= "2024-02-01")
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.status"), F.col("p.paid_at"))
)
