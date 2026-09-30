from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("*").alias("num_accounts"),
    )
    .filter(F.col("balance_range") >= 56)
    .select("customer_id", "balance_range", "num_accounts")
)
