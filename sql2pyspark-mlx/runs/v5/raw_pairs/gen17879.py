from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("island_id").desc())

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-05 01:00:00"))) & (F.col("txn_type") == "DEBIT"))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

with_prev = filtered.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "txn_type_merchant_key",
        "merchant_clean",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

kept_keys = (
    sessions_transactions
    .select("txn_type", "id")
    .subtract(
        df_transactions
        .filter(F.hour("txn_time").between(12, 19))
        .select("txn_type", "id")
    )
)

reconciled = (
    sessions_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "txn_type",
        "txn_time",
        "is_flagged",
        "txn_type_merchant_key",
        "island_id",
        F.avg("island_id").over(w3).alias("island_id_avg_7d"),
        F.dense_rank().over(w4).alias("island_id_dense_rank"),
    )
)

result = (
    enriched
    .select(
        "txn_type",
        "txn_type_merchant_key",
        "island_id",
        "island_id_dense_rank",
        F.when(F.col("txn_type").rlike("^(FEE|DEBIT)$"), "GROUP_B").otherwise("GROUP_A").alias("txn_type_zone"),
    )
)
