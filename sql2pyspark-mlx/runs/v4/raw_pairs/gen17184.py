from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("store_id").isin([3, 5, 20])).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(F.col("p.id"), F.col("p.amount"), F.col("p.status"))
)
