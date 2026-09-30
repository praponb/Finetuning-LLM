from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance_avg_3").desc_nulls_first())
w4 = Window.orderBy("running_max_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("running_avg_balance").desc(), "id")
w6 = Window.partitionBy("account_type").orderBy(F.col("balance_avg_3_dense_rank").asc_nulls_first(), "id")
w7 = Window.partitionBy("status").orderBy(F.col("balance_avg_3_dense_rank").desc(), "id")
w8 = Window.partitionBy("account_type").orderBy("balance_avg_3_dense_rank", "id")
w9 = Window.orderBy(F.col("seq_no").desc_nulls_first())
w10 = Window.orderBy(F.col("next_balance_avg_3_dense_rank").asc_nulls_last())
w11 = Window.orderBy(F.col("balance_avg_3_dense_rank").desc())
w12 = Window.orderBy(F.col("balance_avg_3_dense_rank").desc_nulls_first())
w13 = Window.orderBy(F.col("next_balance_avg_3").desc())

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-08-10").cast("date")) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select("*", F.add_months("opened_date", 1).alias("opened_date_plus_months"))
)

scored = (
    filtered
    .filter(F.col("balance") < 44000)
    .select(
        "*",
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.max("balance").over(w).alias("running_max_balance"),
        F.min("balance").over(w).alias("running_min_balance"),
        F.avg("balance").over(w2).alias("balance_avg_3"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "running_min_balance",
        "balance_avg_3",
        F.dense_rank().over(w3).alias("balance_avg_3_dense_rank"),
        F.sum("running_min_balance").over(w4).alias("cumulative_running_min_balance"),
        F.ntile(4).over(w5).alias("running_avg_balance_bucket"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("id", "account_type", "balance_avg_3", "balance_avg_3_dense_rank")
)

with_windows = (
    ranked
    .select(
        "id",
        "balance_avg_3_dense_rank",
        F.lead("balance_avg_3").over(w8).alias("next_balance_avg_3"),
        F.lead("balance_avg_3_dense_rank").over(w8).alias("next_balance_avg_3_dense_rank"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "balance_avg_3_dense_rank",
        "next_balance_avg_3",
        "next_balance_avg_3_dense_rank",
        "seq_no",
        F.percent_rank().over(w9).alias("seq_no_percent_rank"),
        F.cume_dist().over(w10).alias("next_balance_avg_3_dense_rank_cume_dist"),
        F.dense_rank().over(w11).alias("balance_avg_3_dense_rank_dense_rank"),
        F.dense_rank().over(w12).alias("balance_avg_3_dense_rank_dense_rank_2"),
        F.dense_rank().over(w13).alias("next_balance_avg_3_dense_rank_2"),
    )
)
