from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type", "account_id").orderBy("amount_whole", "id")
w_running = Window.orderBy(F.col("amount_whole").desc_nulls_last())
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount_whole").desc(), "id")
w_grp = Window.partitionBy("account_id").orderBy("amount_whole", "id")
w_part = Window.partitionBy("account_id").orderBy(F.col("least_amount_whole_cume_dist_next_amount_whole").desc())
w_grp_2 = Window.partitionBy("account_id").orderBy(F.col("least_amount_whole_cume_dist_next_amount_whole").desc_nulls_first())

cleaned = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        F.floor("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .filter(F.col("amount_whole") != 43)
    .withColumn("next_amount_whole", F.lead("amount_whole", 1).over(w2))
    .withColumn("amount_whole_cume_dist", F.cume_dist().over(w_running))
    .withColumn("txn_type_max_amount", F.max("amount").over(w4))
    .withColumn("prev_amount", F.lag("amount", 1).over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

cleaned_transactions = (
    enriched
    .filter(F.col("account_id").isin([13, 29, 33, 34]))
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "amount_whole",
        "resolved_merchant",
        "rn",
        "txn_type_max_amount",
        "prev_amount",
        F.least("amount_whole_cume_dist", "next_amount_whole").alias("least_amount_whole_cume_dist_next_amount_whole"),
    )
)

filtered = (
    cleaned_transactions
    .filter(F.col("amount_whole") >= 44)
    .select(
        "id",
        "account_id",
        "amount",
        "amount_whole",
        "resolved_merchant",
        "rn",
        "txn_type_max_amount",
        "least_amount_whole_cume_dist_next_amount_whole",
        F.ceil("rn").alias("rn_whole"),
    )
)

valid = (
    filtered
    .filter(F.col("rn_whole") >= 41)
    .withColumn("rn_per_rn_whole", (F.col("rn") / F.nullif("rn_whole", F.lit(0))))
)

with_windows = (
    valid
    .select(
        "id",
        "amount",
        "amount_whole",
        "resolved_merchant",
        "rn",
        "least_amount_whole_cume_dist_next_amount_whole",
        "rn_whole",
        F.lag("rn_whole", 1).over(w_grp).alias("prev_rn_whole"),
        F.cume_dist().over(w_part).alias("least_amount_whole_cume_dist_next_amount_whole_cume_dist"),
        F.dense_rank().over(w_grp_2).alias("least_amount_whole_cume_dist_next_amount_whole_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "amount_whole",
        "resolved_merchant",
        "rn",
        "least_amount_whole_cume_dist_next_amount_whole",
        "rn_whole",
        "prev_rn_whole",
        "least_amount_whole_cume_dist_next_amount_whole_cume_dist",
    )
)
