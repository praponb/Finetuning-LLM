from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("cs.status_list"))
    .orderBy("signup_date")
)
