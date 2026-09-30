from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("amount", "id")
w2 = Window.partitionBy("status", "account_id")
w3 = Window.partitionBy("account_id").orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "status").orderBy("amount_bucket", "id")
w_running = Window.partitionBy("status", "account_id").orderBy(F.col("amount_share").desc_nulls_last())
w6 = Window.partitionBy("account_id").orderBy(F.col("event_seq").desc())
w7 = Window.partitionBy("week_start").orderBy(F.col("avg_amount_share_percent_rank").desc())

enriched = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.status").isNotNull())
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.merchant",
        "e.is_flagged",
        F.col("d.status"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "account_id",
        "is_flagged",
        "status",
        F.ntile(5).over(w_hist).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "account_id",
        "status",
        "amount_bucket",
        "amount_share",
        F.sum("amount_bucket").over(w3).alias("running_amount_bucket"),
        F.row_number().over(w4).alias("event_seq"),
        F.percent_rank().over(w_running).alias("amount_share_percent_rank"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    deduped
    .select("account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant").isin(["Amazon", "7-Eleven", "Lazada"]))
        .select("account_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.amount_share",
        "src.running_amount_bucket",
        "src.amount_share_percent_rank",
        F.col("b.opened_date"),
    )
)

totals = (
    with_dims
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("amount_share_percent_rank"), 2).alias("avg_amount_share_percent_rank"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.round(F.avg("amount_share_percent_rank"), 2).alias("avg_amount_share_percent_rank_2"),
        F.count("*").alias("row_count"),
    )
    .withColumn("avg_amount_share_percent_rank_dense_rank", F.dense_rank().over(w7))
)

result = (
    totals
    .select(
        "week_start",
        "activity_month",
        "avg_amount_share_percent_rank",
        "row_count",
        "avg_amount_share_percent_rank_dense_rank",
    )
    .orderBy(F.col("week_start").desc())
)
