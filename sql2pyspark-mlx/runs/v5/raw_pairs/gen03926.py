from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance_rounded").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance_rounded").desc())
w3 = Window.partitionBy("customer_id").orderBy("balance", "id")
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.orderBy("balance", "id")

cleaned = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & (F.col("customer_id") == 37))
    .select(
        "*",
        F.round("balance", 2).alias("balance_rounded"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .filter(F.col("status") != "FROZEN")
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "resolved_status",
        F.rank().over(w2).alias("balance_rounded_rank"),
        F.lead("balance", 1).over(w3).alias("next_balance"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isNull()), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.lag("balance_rounded_rank", 1).over(w5).alias("prev_balance_rounded_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.when(F.col("next_balance") >= 83000, "PREMIUM").otherwise("STANDARD").alias("next_balance_tier"),
        "account_type",
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_per_checking"),
    )
)
