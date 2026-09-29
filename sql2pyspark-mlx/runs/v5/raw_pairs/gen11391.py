from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.city").alias("city"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.balance")).alias("max_balance"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("max_balance").desc())
)
