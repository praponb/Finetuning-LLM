from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w_recent = Window.partitionBy("account_id", "merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("account_id")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount")
w6 = Window.partitionBy("account_id").orderBy(F.col("event_seq").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy("amount_share", "id")

with_windows = (
    df_transactions
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

valid = (
    with_windows
    .filter((F.col("txn_time") > F.to_timestamp(F.lit("2023-11-02 01:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "amount_share",
        "event_seq",
        "amount_cume_dist",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

scored = (
    valid
    .withColumn("merchant_sum_amount", F.sum("amount").over(w))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4))
)

metrics = (
    scored
    .select(
        "*",
        F.cume_dist().over(w5).alias("amount_cume_dist_2"),
        F.row_number().over(w6).alias("row_seq"),
        F.row_number().over(w7).alias("event_seq_2"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("merchant_sum_amount") >= 21213, "HIGH").otherwise("LOW").alias("merchant_sum_amount_tier"),
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
        F.when(F.col("amount_cume_dist") >= 0.81, "PREMIUM").otherwise("STANDARD").alias("amount_cume_dist_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.count_if(F.col("merchant") == "Amazon").alias("amazon_events"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
    )
    .filter(F.col("debit_count") >= 1)
    .orderBy(F.col("row_seq_tier").desc())
)
