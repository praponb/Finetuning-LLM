from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.balance")).alias("min_balance"),
    )
    .orderBy("city")
)
