from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .cube(
        F.col("p.country").alias("country"),
        F.col("s.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.balance")).alias("total_balance"),
    )
)
