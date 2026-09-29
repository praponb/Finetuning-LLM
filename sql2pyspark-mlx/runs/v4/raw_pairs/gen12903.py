from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CHECKING"]))
    .withColumn("status_filled", F.coalesce("status", F.lit("unassigned")))
)

result = (
    valid
    .groupBy("customer_id", "status")
    .pivot("account_type", ["CREDIT", "LOAN", "CHECKING"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
