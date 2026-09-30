from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "txn_type")
    .pivot("merchant", ["7-Eleven", "Amazon"])
    .agg(F.avg("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "account_id",
        "txn_type",
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_cnt").alias("v_7_eleven_cnt"),
        F.col("Amazon_value_sum").alias("amazon_value_sum"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
    )
)
