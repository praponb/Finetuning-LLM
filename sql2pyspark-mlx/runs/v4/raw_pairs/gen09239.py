from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

matched_keys = (
    islands
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("account_id").isin([11, 13, 17, 22]))
        .select("txn_type", "merchant")
    )
)

verified = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-12-30 13:00:00")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "new_group_flag",
        "burst_id",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

result = (
    filtered
    .select("id", "txn_time", "new_group_flag", "burst_id", "txn_type_title")
    .orderBy(F.col("new_group_flag").desc())
)
