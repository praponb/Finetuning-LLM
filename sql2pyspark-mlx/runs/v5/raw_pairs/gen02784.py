from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-01"))) & (F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 4))
    .withColumn("status_filled", F.coalesce("status", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .groupBy("customer_id")
    .pivot("account_type", ["CHECKING", "LOAN"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
