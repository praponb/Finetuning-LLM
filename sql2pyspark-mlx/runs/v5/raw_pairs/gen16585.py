from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-06 04:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

result = (
    filtered
    .withColumn("amount_tier", F.when(F.col("amount") >= 3800, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["merchant", "amount_tier"], ["merchant"]],
        "merchant",
        "amount_tier",
    )
    .agg(
        F.min("txn_time").alias("first_seen"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("duration_min").desc())
)
