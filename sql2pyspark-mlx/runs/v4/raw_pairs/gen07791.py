from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("is_grab", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last_known_is_grab")
w5 = Window.partitionBy("account_id", "status")
w_part = Window.partitionBy("merchant", "status").orderBy("cumulative_is_grab", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("account_id", "status").orderBy("is_grab_avg_6")

cleaned = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("is_grab", F.when(F.col("merchant") == "Grab", 1).otherwise(0))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("DIRECT")))
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.status"), F.col("b.account_type"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("is_grab").over(w_recent).alias("cumulative_is_grab"),
        F.count("is_grab").over(w2).alias("is_grab_count_30d"),
        F.last("is_grab", ignorenulls=True).over(w3).alias("last_known_is_grab"),
    )
)

metrics = (
    with_windows
    .withColumn("last_known_is_grab_rank", F.rank().over(w4))
    .withColumn("account_id_sum_is_grab", F.sum("is_grab").over(w5))
    .withColumn("is_grab_avg_6", F.avg("is_grab").over(w_part))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.opened_date"),
        F.col("b.status").alias("account_status"),
        F.col("b.account_type").alias("account_account_type"),
    )
)

result = (
    enriched
    .cube("merchant")
    .agg(
        F.count(F.when(F.col("account_account_type") == "CHECKING", 1)).alias("checking_count"),
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.avg("is_grab_count_30d"), 2).alias("avg_is_grab_count_30d"),
        F.grouping_id().alias("gid"),
    )
)
