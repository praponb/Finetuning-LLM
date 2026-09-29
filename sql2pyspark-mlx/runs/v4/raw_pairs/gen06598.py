from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-22 00:00:00").cast("timestamp")) & F.col("merchant").isin(["Grab", "Shell"]))
    .select(
        "*",
        F.lower("txn_type").alias("txn_type_lower"),
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

cleaned = (
    valid
    .filter(F.col("txn_time") >= F.lit("2023-12-08 22:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "txn_type_lower",
        F.least("amount_int", "amount").alias("least_amount_int_amount"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

valid_transactions = (
    cleaned
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") != "Amazon"))
    .withColumn("least_amount_int_amount_int", F.col("least_amount_int_amount").cast("int"))
)

joined = (
    valid_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.account_type"))
)

result = (
    joined
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "txn_type_lower",
        "least_amount_int_amount",
        "least_amount_int_amount_int",
        "account_type",
    )
    .orderBy(F.col("least_amount_int_amount_int").desc())
)
