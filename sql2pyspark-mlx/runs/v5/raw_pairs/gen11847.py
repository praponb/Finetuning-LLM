from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.opened_date") >= "2023-11-01")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("a.status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
    .orderBy("pct_closed", "segment")
    .limit(1)
)
