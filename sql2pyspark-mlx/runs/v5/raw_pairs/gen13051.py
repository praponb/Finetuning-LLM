from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.year("opened_date") >= 2024)
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("cs.account_type_values"),
    )
    .orderBy("signup_date")
)
