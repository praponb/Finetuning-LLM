from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-09-26 13:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select("*", F.upper("merchant").alias("merchant_upper"))
)

with_gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_time",
        "merchant",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

filtered = (
    islands
    .filter(F.col("merchant").isNotNull())
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

with_gaps_2 = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps_2.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_gaps_3 = (
    sessions_transactions
    .withColumn("is_new_burst_2", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions_2 = with_gaps_3.withColumn("session_id", F.sum("is_new_burst_2").over(w2))

matched_keys = (
    sessions_transactions_2
    .select("merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type") == "CREDIT")
        .select("merchant")
    )
)

kept_rows = (
    sessions_transactions_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("is_new_burst_2_tier", F.when(F.col("is_new_burst_2") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["is_new_burst_2_tier", "island_id"], ["is_new_burst_2_tier"]],
        "is_new_burst_2_tier",
        "island_id",
    )
    .agg(
        F.min("is_new_burst_2").alias("min_is_new_burst_2"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
    )
)
