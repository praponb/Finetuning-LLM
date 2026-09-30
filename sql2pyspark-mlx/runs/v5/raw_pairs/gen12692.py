from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-18").cast("date")) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .withColumn("status_lower", F.lower("status"))
    .withColumn("account_type_title", F.initcap(F.trim("account_type")))
)

result = (
    valid
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "OPEN"])
    .agg(F.avg("balance").alias("amount"), F.count(F.lit(1)).alias("n"))
)
