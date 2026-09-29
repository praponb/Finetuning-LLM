from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .pivot("merchant", ["7-Eleven", "Amazon"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_id",
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_n").alias("v_7_eleven_n"),
        F.col("Amazon_value_sum").alias("amazon_value_sum"),
        F.col("Amazon_n").alias("amazon_n"),
    )
)
