from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.city").alias("city"), F.col("p.country").alias("country"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.balance")).alias("max_balance"),
    )
    .orderBy("country")
)
