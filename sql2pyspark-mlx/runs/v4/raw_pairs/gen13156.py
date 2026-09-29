from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "LOAN", "CHECKING"])
    .agg(F.sum("balance").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        F.col("CREDIT_value_sum").alias("credit_value_sum"),
        F.col("CREDIT_cnt").alias("credit_cnt"),
        F.col("LOAN_value_sum").alias("loan_value_sum"),
        F.col("LOAN_cnt").alias("loan_cnt"),
        F.col("CHECKING_value_sum").alias("checking_value_sum"),
        F.col("CHECKING_cnt").alias("checking_cnt"),
    )
)
