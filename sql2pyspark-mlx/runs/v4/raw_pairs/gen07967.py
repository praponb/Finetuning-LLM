from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(
        F.count("*").alias("row_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.account_type")))).alias("account_type_values"),
    )
)
