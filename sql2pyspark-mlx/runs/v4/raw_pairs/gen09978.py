from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-04-19 15:00:00").cast("timestamp")) & F.col("txn_type").isin(["REFUND", "FEE"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status"), F.col("p.opened_date"))
)

filtered_transactions = (
    with_dims
    .filter((F.col("txn_time") > F.lit("2023-11-20").cast("date")) & F.col("status").isNotNull() & (F.col("txn_type") == "REFUND"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "resolved_merchant",
        F.substring("txn_type", 1, 4).alias("txn_type_prefix"),
    )
)

deduped = (
    filtered_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    deduped
    .filter(F.col("txn_time") >= F.lit("2024-05-24").cast("date"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "resolved_merchant",
        "txn_type_prefix",
        F.when(F.col("amount") >= 3500, "High").when(F.col("amount") >= 1300, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w3))

result = (
    sessionized
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "txn_type_prefix",
        "amount_tier",
        "session_start_flag",
        "island_id",
        F.when(F.col("merchant").rlike("^(Starbucks|Lazada)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)
