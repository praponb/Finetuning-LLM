from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.opened_date") <= "2024-01-01")
    .groupBy(F.col("c.segment"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("a.status")))).alias("status_values"),
    )
    .orderBy("status_values", "segment")
    .limit(50)
)
