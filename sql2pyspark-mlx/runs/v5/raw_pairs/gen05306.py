from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .pivot("account_type", ["LOAN", "SAVINGS", "CREDIT"])
    .agg(F.max("balance"))
    .select(
        "customer_id",
        F.col("LOAN").alias("loan"),
        F.col("SAVINGS").alias("savings"),
        F.col("CREDIT").alias("credit"),
    )
)
