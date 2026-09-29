from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance_whole", "id")
w_grp = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("status").orderBy("balance")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy("next_balance_whole", "id")

joined = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.txn_time",
        "src.merchant",
        F.col("p.status"),
        F.col("p.balance"),
    )
)

base = (
    joined
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & (F.col("balance") <= 77000))
    .select(
        "*",
        F.to_date("txn_time").alias("txn_time_date"),
        F.floor("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "txn_time",
        "status",
        "balance",
        F.lead("balance_whole", 1).over(w).alias("next_balance_whole"),
        F.avg("balance").over(w_grp).alias("balance_avg_5_prior"),
    )
)

filtered = (
    with_windows
    .filter((F.col("txn_time") > F.lit("2023-07-15 13:00:00").cast("timestamp")) & F.col("status").isNotNull())
    .withColumn("status_group", F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Core").otherwise("Other"))
    .withColumn("is_frozen", F.when(F.col("status") == "FROZEN", 1).otherwise(0))
    .withColumn("resolved_status", F.coalesce("status", F.lit("NONE")))
)

with_windows_transactions = (
    filtered
    .select(
        "id",
        "status",
        "next_balance_whole",
        "is_frozen",
        "resolved_status",
        F.dense_rank().over(w_ordered).alias("balance_dense_rank"),
        F.min("balance_avg_5_prior").over(w4).alias("status_min_balance_avg_5_prior"),
        F.ntile(4).over(w5).alias("next_balance_whole_bucket"),
    )
)

valid = (
    with_windows_transactions
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "status",
        "next_balance_whole",
        "is_frozen",
        "status_min_balance_avg_5_prior",
        "next_balance_whole_bucket",
        F.col("next_balance_whole").cast("double").alias("next_balance_whole_num"),
        (F.col("status_min_balance_avg_5_prior") + F.col("next_balance_whole_bucket")).alias("status_min_balance_avg_5_prior_plus_next_balance_whole_bucket"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

result = (
    valid
    .withColumn("next_balance_whole_tier", F.when(F.col("next_balance_whole") >= 71, "HIGH").otherwise("LOW"))
    .withColumn("next_balance_whole_num_tier", F.when(F.col("next_balance_whole_num") >= 89, "HIGH").otherwise("LOW"))
    .withColumn("next_balance_whole_bucket_tier", F.when(F.col("next_balance_whole_bucket") >= 7, "HIGH").otherwise("LOW"))
    .cube(
        "next_balance_whole_tier",
        "next_balance_whole_num_tier",
        "next_balance_whole_bucket_tier",
    )
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.round(F.avg("status_min_balance_avg_5_prior_plus_next_balance_whole_bucket"), 2).alias("avg_status_min_balance_avg_5_prior_plus_next_balance_whole_bucket"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.round(F.sum("is_frozen") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("is_frozen_per_open"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("open_count").asc_nulls_first())
)
