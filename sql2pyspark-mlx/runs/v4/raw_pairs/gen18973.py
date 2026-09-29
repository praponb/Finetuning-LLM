from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("customer_id") == 32).select("id")

result = (
    df_payments.alias("p")
    .join(excluded_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("p.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .filter(F.col("p.method") == "bank_transfer")
    .select(F.col("p.id"), F.col("p.amount"))
    .orderBy(F.col("amount").desc(), F.col("id").desc())
)
