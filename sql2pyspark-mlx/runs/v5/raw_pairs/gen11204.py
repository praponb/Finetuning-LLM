from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.order_date") <= "2024-05-01")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count(F.when(F.col("p.status") == "SUCCESS", 1)).alias("success_count"),
        F.count("*").alias("cnt"),
    )
)
