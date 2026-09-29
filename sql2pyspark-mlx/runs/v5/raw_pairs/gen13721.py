from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("a.status"))).alias("status_list"),
        F.min(F.col("a.opened_date")).alias("min_opened_date"),
    )
)
