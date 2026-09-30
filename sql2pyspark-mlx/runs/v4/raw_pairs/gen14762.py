from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.orderBy("amount", "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w6 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy(F.col("is_new_burst").asc_nulls_first(), "id")

with_windows = (
    df_transactions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
        F.lag("amount", 1).over(w4).alias("prev_amount_2"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.status"))
)

gaps = (
    joined
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w6))

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.account_type"),
        F.col("b.balance"),
        F.col("b.status").alias("account_status"),
    )
)

stats = (
    enriched
    .groupBy(
        "account_status",
        "account_type",
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
    )
)

result = (
    stats
    .select(
        "account_status",
        "account_type",
        "year_start",
        "savings_count",
        "island_id_quartiles",
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
