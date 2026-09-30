from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "status"], ["segment"]],
        F.col("c.segment").alias("segment"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.balance")).alias("min_balance"),
    )
    .orderBy(F.col("min_balance").asc_nulls_last())
)
