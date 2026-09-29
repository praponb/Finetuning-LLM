from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-11-12"))) & F.col("amount").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .select("id", "merchant")
    .exceptAll(
        df_transactions
        .filter(F.col("merchant") == "Shell")
        .select("id", "merchant")
    )
)
