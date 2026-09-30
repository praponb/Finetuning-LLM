from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.email"),
        F.col("customers.signup_date"),
        F.col("customer_summary.status_values"),
    )
)
