from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id")
w_running = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("account_id") == 5)
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.sum("amount").over(w_running).alias("running_amount"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

prev_events = with_windows.withColumn("prev_txn_time", F.lag("txn_time").over(w4))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w5))

base = (
    islands
    .filter(F.col("is_new_burst") < 1)
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "txn_type_title",
        "resolved_merchant",
        "seq_no",
        "running_amount",
        "prev_txn_time",
        "is_new_burst",
        "island_id",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.round("island_id", 0).alias("island_id_rounded"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"), F.col("d.account_type"), F.col("d.balance"))
)

with_dims_transactions = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.account_type").alias("account_account_type"))
)

result = (
    with_dims_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "txn_type_title",
        "seq_no",
        "prev_txn_time",
        "is_new_burst",
        F.when(F.col("merchant").rlike("^(Starbucks|Amazon)$"), "GROUP_A").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "CORE").otherwise("EXTENDED").alias("merchant_zone"),
    )
)
