from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w_running = Window.partitionBy("account_id", "txn_type").orderBy("amount_int", "id")
w3 = Window.partitionBy("merchant").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-23").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .withColumn("next_amount", F.lead("amount").over(w_running))
    .withColumn("next_amount_int", F.lead("amount_int").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
)

with_windows = (
    scored
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "next_amount",
        "next_amount_int",
        "seq_no",
        F.max("seq_no").over(w3).alias("running_max_seq_no"),
        F.min("seq_no").over(w3).alias("running_min_seq_no"),
        F.count("seq_no").over(w3).alias("running_count_seq_no"),
    )
)

valid_transactions = (
    with_windows
    .filter(F.col("merchant").isNotNull() & (F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "next_amount",
        "next_amount_int",
        "seq_no",
        "running_max_seq_no",
        "running_count_seq_no",
        F.ceil("seq_no").alias("seq_no_whole"),
        F.least("running_count_seq_no", "running_min_seq_no").alias("least_running_count_seq_no_running_min_seq_no"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

result = (
    valid_transactions
    .select(
        "id",
        "account_id",
        "merchant",
        "next_amount",
        "running_max_seq_no",
        "seq_no_whole",
        "least_running_count_seq_no_running_min_seq_no",
        F.when(F.col("merchant").rlike("^(Grab|Starbucks)$"), "GROUP_A").otherwise("CORE").alias("merchant_zone"),
    )
    .orderBy(F.col("running_max_seq_no").desc_nulls_last(), "merchant_zone")
)
