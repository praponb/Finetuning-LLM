from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "inner")
    .filter(F.col("customer_totals.cnt") > 10)
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.age"),
        F.col("customer_totals.cnt"),
        F.col("customer_totals.last_order_date"),
    )
    .orderBy("last_order_date", "id")
    .limit(5)
)
