from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w_running = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w4 = Window.partitionBy("account_id")
w5 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("seq_no").desc())
w_grp = Window.partitionBy("txn_type")
w7 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())

enriched = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.ntile(10).over(w2).alias("amount_bucket"),
        F.row_number().over(w_running).alias("seq_no"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
)

filtered = (
    ranked
    .filter(F.col("txn_time") > F.lit("2023-05-09 16:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "running_amount",
        "amount_bucket",
        "seq_no",
        "amount_share",
        "rn",
        (F.col("amount") + F.col("amount_bucket")).alias("amount_plus_amount_bucket"),
        F.greatest("amount_bucket", "running_amount").alias("greatest_amount_bucket_running_amount"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.balance"), F.col("b.opened_date"))
)

scored = (
    joined
    .withColumn("txn_type_max_seq_no", F.max("seq_no").over(w_grp))
    .withColumn("amount_cume_dist", F.cume_dist().over(w7))
)

result = (
    scored
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "running_amount",
        "amount_bucket",
        "amount_share",
        "rn",
        "amount_plus_amount_bucket",
    )
    .orderBy(F.col("running_amount").asc_nulls_first(), F.col("rn").desc())
)
