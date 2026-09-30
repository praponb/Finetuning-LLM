from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("next_amount").asc_nulls_first())
w4 = Window.orderBy("next_amount", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("seq_no").desc(), "id")

enriched = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("seq_no"),
        F.count("amount").over(w2).alias("amount_count_90d"),
    )
)

base = (
    enriched
    .filter((F.col("txn_time") >= F.lit("2023-04-25").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "next_amount",
        "seq_no",
        "amount_count_90d",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        (F.col("amount_count_90d") + F.col("seq_no")).alias("amount_count_90d_plus_seq_no"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

metrics = (
    base
    .withColumn("next_amount_dense_rank", F.dense_rank().over(w3))
    .withColumn("amount_count_90d_avg_5", F.avg("amount_count_90d").over(w4))
    .withColumn("seq_no_bucket", F.ntile(3).over(w5))
)

remaining_keys = (
    metrics
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("account_id") == 7)
        .select("id", "account_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "next_amount",
        "seq_no",
        "amount_count_90d",
        "txn_time_day",
        "amount_count_90d_plus_seq_no",
    )
    .orderBy(F.col("id").desc(), "seq_no")
)
