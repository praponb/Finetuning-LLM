from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first(), "id")
w_part = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("new_group_flag", "id")
w6 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").desc_nulls_last())
w_part_2 = Window.partitionBy("account_id")
w8 = Window.partitionBy("txn_type")

with_dims = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        F.col("a.balance"),
    )
)

gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.sum("new_group_flag").over(w_part).alias("new_group_flag_sum_30d"),
        F.lag("island_id", 2, 0).over(w5).alias("prev_island_id"),
        F.percent_rank().over(w6).alias("amount_percent_rank"),
        F.stddev_samp("balance").over(w_part_2).alias("account_id_stddev_samp_balance"),
        F.round((F.col("amount") / F.sum("amount").over(w8)), 4).alias("amount_share"),
    )
)

grouped = (
    metrics
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "HIGH").otherwise("LOW"))
    .rollup("new_group_flag_tier", "account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.min("prev_island_id").alias("min_prev_island_id"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("balance"))).alias("credit_total_balance"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .select(
        "new_group_flag_tier",
        "avg_amount",
        "distinct_txn_types",
        "min_prev_island_id",
        "distinct_account_ids",
        "credit_total_balance",
        "gid",
    )
    .orderBy("avg_amount")
)
