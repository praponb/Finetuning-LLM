from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-12-07").cast("date")) & F.col("customer_id").isNotNull() & (F.col("account_type") == "CREDIT"))
    .withColumn("clean_account_type", F.trim("account_type"))
)

result = (
    filtered
    .groupBy("account_type", "customer_id")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.max("balance").alias("total"), F.count(F.lit(1)).alias("n"))
)
