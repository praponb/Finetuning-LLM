from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("merchant", ["7-Eleven", "Starbucks", "Lazada"])
    .agg(F.sum("amount").alias("amount"))
    .select(
        "account_id",
        F.col("7-Eleven").alias("v_7_eleven"),
        F.col("Starbucks").alias("starbucks"),
        F.col("Lazada").alias("lazada"),
    )
)
