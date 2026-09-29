from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant", "account_id").orderBy("running_max_amount", "id")

enriched = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.min("amount").over(w_hist).alias("running_min_amount"),
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
        F.max("amount").over(w_hist).alias("running_max_amount"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lag("running_max_amount").over(w2).alias("prev_running_max_amount"),
        F.lead("running_max_amount").over(w2).alias("next_running_max_amount"),
        F.lead("amount").over(w2).alias("next_amount"),
        F.lag("amount").over(w2).alias("prev_amount"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("amount") >= 1600)
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("next_running_max_amount") < 3900)
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "running_min_amount",
        "running_sum_amount",
        "running_max_amount",
        "prev_running_max_amount",
        "next_running_max_amount",
        "next_amount",
        "event_seq",
        (F.col("amount") * F.col("event_seq")).alias("amount_times_event_seq"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("next_running_max_amount") >= 3200, "LARGE").otherwise("SMALL").alias("next_running_max_amount_tier"),
    )
    .agg(
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.round(F.sum("next_amount") / F.nullif(F.count(F.when(F.col("merchant") == "Shell", 1)), F.lit(0)), 2).alias("next_amount_per_shell"),
        F.round(F.avg("running_max_amount"), 2).alias("avg_running_max_amount"),
    )
    .filter(F.col("avg_running_max_amount") >= 0)
    .orderBy(F.col("next_running_max_amount_tier").desc_nulls_last())
)
