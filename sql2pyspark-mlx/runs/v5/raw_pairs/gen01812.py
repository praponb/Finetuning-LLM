from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.signup_date"),
        F.col("cs.first_opened_date"),
        F.col("cs.status_list"),
    )
    .orderBy("signup_date", F.col("first_opened_date").desc())
)
