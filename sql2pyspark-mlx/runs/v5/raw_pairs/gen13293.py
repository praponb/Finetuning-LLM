from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.account_type")))).alias("account_type_values"),
    )
    .orderBy("account_type_values")
)
