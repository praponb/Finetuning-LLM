from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("num_accounts"),
        F.count(F.when(F.col("a.account_type") == "CREDIT", 1)).alias("credit_count"),
    )
)
