from pyspark.sql import functions as F

by_dimension = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city").alias("city"), F.col("c.segment").alias("segment"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
    )
)

result = by_dimension.select("city", "segment", "num_rows", "avg_balance")
