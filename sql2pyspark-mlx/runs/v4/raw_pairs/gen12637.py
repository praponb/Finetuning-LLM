from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("status").isNull())
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("open_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("t.open_count"),
        F.col("t.account_type_values"),
    )
    .orderBy(F.col("open_count").desc())
)
