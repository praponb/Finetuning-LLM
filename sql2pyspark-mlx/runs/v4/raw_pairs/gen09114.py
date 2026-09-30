from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.savings_count") >= 78)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.age"),
        F.col("t.savings_count"),
        F.col("t.account_type_values"),
    )
    .orderBy(F.col("id").desc(), "savings_count")
)
