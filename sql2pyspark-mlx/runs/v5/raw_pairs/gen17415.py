from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("merchant", "status").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy(F.col("balance").desc())
w6 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last())
w7 = Window.partitionBy("merchant").orderBy(F.col("amount_dense_rank").desc(), "id")
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("status").orderBy("amount_avg_3_prior", "id")
w10 = Window.partitionBy("merchant", "status")
w_part = Window.partitionBy("merchant").orderBy("amount_dense_rank_bucket", "id")
w_grp = Window.partitionBy("merchant").orderBy(F.col("next_amount_avg_3_prior").desc(), "id")
w13 = Window.partitionBy("status").orderBy("amount_avg_3_prior_share", "id")
w14 = Window.partitionBy("status").orderBy("amount_avg_3_prior_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-08-01").cast("date")) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_trunc("MONTH", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.merchant").isin(["Amazon", "Lazada"])), "left_semi")
)

latest_match = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date_period")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.status",
        "s.opened_date_period",
        "s.status_clean",
        F.col("c.merchant"),
        F.col("c.amount"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.rank().over(w2).alias("amount_rank"),
        F.avg("amount").over(w3).alias("amount_avg_3_prior"),
        F.ntile(10).over(w4).alias("amount_bucket"),
        F.dense_rank().over(w5).alias("balance_dense_rank"),
        F.dense_rank().over(w6).alias("amount_dense_rank"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date_period")), "left_anti")
)

metrics = (
    not_flagged
    .filter(F.col("amount") < 3800)
    .select(
        "id",
        "status",
        "status_clean",
        "merchant",
        "amount_bucket",
        "balance_dense_rank",
        F.ntile(5).over(w7).alias("amount_dense_rank_bucket"),
        F.sum("amount_bucket").over(w8).alias("status_sum_amount_bucket"),
        F.lead("amount_avg_3_prior", 1).over(w9).alias("next_amount_avg_3_prior"),
        F.round((F.col("amount_avg_3_prior") / F.sum("amount_avg_3_prior").over(w10)), 4).alias("amount_avg_3_prior_share"),
    )
)

scored_accounts = (
    metrics
    .select(
        "id",
        "status",
        "balance_dense_rank",
        "amount_avg_3_prior_share",
        F.lead("status_sum_amount_bucket").over(w_part).alias("next_status_sum_amount_bucket"),
        F.lag("status_sum_amount_bucket").over(w_part).alias("prev_status_sum_amount_bucket"),
        F.lag("amount_dense_rank_bucket").over(w_part).alias("prev_amount_dense_rank_bucket"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(4).over(w_grp).alias("next_amount_avg_3_prior_bucket"),
    )
)

result = (
    scored_accounts
    .select(
        "id",
        "status",
        "balance_dense_rank",
        "prev_status_sum_amount_bucket",
        "row_seq",
        "next_amount_avg_3_prior_bucket",
        F.round((F.col("prev_status_sum_amount_bucket") / F.sum("prev_status_sum_amount_bucket").over(w8)), 4).alias("prev_status_sum_amount_bucket_share"),
        F.lead("row_seq", 2).over(w13).alias("next_row_seq"),
        F.last("balance_dense_rank", ignorenulls=True).over(w14).alias("last_known_balance_dense_rank"),
    )
)
