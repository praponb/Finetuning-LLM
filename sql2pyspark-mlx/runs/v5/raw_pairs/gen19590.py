from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("balance").isNotNull() & F.col("status").isin(["OPEN", "CLOSED"]))
    .withColumn("clean_status", F.trim("status"))
)

summary = (
    base
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
)

result = (
    summary
    .filter(F.col("savings_count") > 69)
    .select("status", "savings_count")
    .orderBy(F.col("status").desc())
)
