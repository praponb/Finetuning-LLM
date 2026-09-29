from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.balance")).alias("min_balance"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
