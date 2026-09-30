from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").eqNullSafe(F.lit("OPEN")))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 12))
    .withColumn("status_clean", F.coalesce("status", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .groupBy("customer_id", "status")
    .pivot("account_type", ["CHECKING", "SAVINGS"])
    .agg(F.sum("balance").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
