from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.balance")).alias("max_balance"),
    )
)
