from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.min_balance") > 55000)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("t.min_balance"),
        F.col("t.status_list"),
    )
)
