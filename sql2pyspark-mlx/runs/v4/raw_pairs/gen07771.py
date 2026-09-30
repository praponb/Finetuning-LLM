from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.credit_limit"),
        F.col("customer_summary.account_type_values"),
    )
    .orderBy("id")
)
