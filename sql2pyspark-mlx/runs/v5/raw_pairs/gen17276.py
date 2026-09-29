from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "customer_id")
    .pivot("account_type", ["CHECKING", "SAVINGS"])
    .agg(F.avg("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        "customer_id",
        F.col("CHECKING_amount").alias("checking_amount"),
        F.col("CHECKING_cnt").alias("checking_cnt"),
        F.col("SAVINGS_amount").alias("savings_amount"),
        F.col("SAVINGS_cnt").alias("savings_cnt"),
    )
)
