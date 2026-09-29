from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1410)
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .filter(F.col("failed_amount") > 48)
    .select("status", "pct_success", "failed_amount")
    .orderBy(F.col("pct_success").desc())
)
