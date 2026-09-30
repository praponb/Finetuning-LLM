from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CHECKING"]))
    .select("*", F.trim("status").alias("clean_status"))
)

result = (
    valid
    .groupBy("customer_id")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
