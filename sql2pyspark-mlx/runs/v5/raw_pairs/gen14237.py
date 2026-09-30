from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy("burst_id")
w_ordered = Window.partitionBy("account_id")

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

remaining_keys = (
    islands
    .select("id")
    .subtract(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "account_id",
        "merchant",
        "burst_id",
        F.percent_rank().over(w3).alias("burst_id_percent_rank"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.balance"))
)

enriched_transactions = (
    enriched
    .select(
        "account_id",
        "merchant",
        "burst_id_percent_rank",
        "balance",
        F.avg("burst_id").over(w_ordered).alias("account_id_avg_burst_id"),
    )
)

result = (
    enriched_transactions
    .groupBy("merchant")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
    )
    .filter(F.col("amazon_count") < 1000)
    .orderBy("amazon_count")
)
