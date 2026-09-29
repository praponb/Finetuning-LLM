from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .cube(
        F.col("c.city").alias("city"),
        F.col("src.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.balance")).alias("max_balance"),
        F.grouping_id().alias("grouping_level"),
    )
)
