from pyspark.sql import functions as F

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & ~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
        F.trim("merchant").alias("clean_merchant"),
    )
)

base = (
    filtered
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-04-10 20:00:00")))
    .select(
        "id",
        "txn_type",
        "merchant",
        "txn_type_prefix",
        "clean_merchant",
        F.lower("merchant").alias("merchant_lower"),
    )
)

filtered_transactions = (
    base
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "merchant",
        "txn_type_prefix",
        "clean_merchant",
        "merchant_lower",
        F.when(F.col("merchant").isin(["Grab", "Lazada"]), "Priority").otherwise("Other").alias("merchant_group"),
    )
)

valid = (
    filtered_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .withColumn("merchant_txn_type", F.concat("merchant", F.lit("/"), "txn_type"))
)

daily = (
    valid
    .groupBy("merchant")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count_2"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
    )
    .filter(F.col("num_txn_types") > 1)
)

result = (
    daily
    .select(
        "merchant",
        "num_rows",
        "7_eleven_count",
        F.when(F.col("merchant").rlike("^(Grab|Amazon)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "GROUP_A").otherwise("PRIORITY").alias("merchant_zone"),
    )
    .orderBy(F.col("merchant").desc_nulls_last())
    .limit(20)
)
