from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("amount_share").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("first_known_amount", "id")
w6 = Window.orderBy("amount", "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w8 = Window.partitionBy("merchant").orderBy("amount_share_2", "id")
w9 = Window.partitionBy("merchant").orderBy("last_known_amount_share_bucket", "id").rowsBetween(-5, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2024-04-10"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

scored = (
    enriched
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w3))
    .withColumn("amount_share_bucket", F.ntile(5).over(w_running))
)

enriched_transactions = (
    scored
    .select(
        "*",
        F.last("amount_share_bucket", ignorenulls=True).over(w3).alias("last_known_amount_share_bucket"),
        F.ntile(3).over(w5).alias("first_known_amount_bucket"),
        F.ntile(10).over(w6).alias("amount_bucket"),
        F.dense_rank().over(w7).alias("amount_dense_rank"),
        F.lead("amount_share_2", 2).over(w8).alias("next_amount_share_2"),
    )
)

scored_transactions = (
    enriched_transactions
    .withColumn("last_known_amount_share_bucket_avg_5", F.avg("last_known_amount_share_bucket").over(w9))
    .withColumn("seq_no", F.row_number().over(w))
)

result = (
    scored_transactions
    .select(
        "id",
        "is_flagged_label",
        "amount_share",
        "first_known_amount",
        "amount_share_bucket",
        "last_known_amount_share_bucket",
        "amount_bucket",
        "next_amount_share_2",
        "last_known_amount_share_bucket_avg_5",
    )
    .orderBy("amount_bucket")
)
