from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("account_id").orderBy("txn_time", "id")
w_part_2 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("merchant", "account_id")
w6 = Window.partitionBy("merchant").orderBy(F.col("amount_share").desc(), "id")
w7 = Window.partitionBy("account_id", "merchant").orderBy(F.col("latest_rank").desc(), "id")
w8 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc_nulls_first())

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lead("amount", 1, 0).over(w_part).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5"),
        F.lead("amount", 1, 0).over(w_running).alias("next_amount_2"),
        F.row_number().over(w_part_2).alias("event_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w5)), 4).alias("amount_share"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "next_amount",
        "amount_avg_5",
        "amount_share",
        "latest_rank",
    )
)

ranked_transactions = (
    latest
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
    .select("id", "txn_type", "merchant")
)

filtered = (
    ranked_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "*",
        F.lower("txn_type").alias("txn_type_lower"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

base = (
    filtered
    .filter(F.col("merchant").isNotNull() & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "txn_type_lower",
        "resolved_merchant",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)

result = (
    base
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
    )
    .filter(F.col("cnt") < 1000)
)
