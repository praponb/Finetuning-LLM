from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.country").alias("country"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.balance")), 2).alias("total_balance"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
