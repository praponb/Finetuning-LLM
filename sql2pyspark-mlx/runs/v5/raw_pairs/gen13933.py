from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc())
w2 = Window.orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("balance_whole", "id")
w4 = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w5 = Window.partitionBy("status")
w6 = Window.orderBy(F.col("balance").desc())
w7 = Window.partitionBy("customer_id", "account_type").orderBy("balance_bucket", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("customer_id").orderBy("balance_bucket", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("opened_date") > "2024-04-01")
    .withColumn("balance_whole", F.ceil("balance"))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([15, 22])), "left_semi")
)

with_windows = (
    eligible
    .filter(F.col("balance_whole") == 28)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.avg("balance_whole").over(w2).alias("balance_whole_avg_6"),
        F.lead("balance_whole", 2).over(w_running).alias("next_balance_whole"),
        F.ntile(4).over(w4).alias("balance_bucket"),
        F.sum("balance_whole").over(w5).alias("status_sum_balance_whole"),
        F.percent_rank().over(w6).alias("balance_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "next_balance_whole",
        "balance_bucket",
        "status_sum_balance_whole",
        "balance_percent_rank",
        F.avg("balance_bucket").over(w7).alias("balance_bucket_avg_3"),
        F.avg("balance").over(w8).alias("balance_avg_3"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("next_balance_whole") >= 71, "LARGE").otherwise("SMALL").alias("next_balance_whole_tier"),
    )
    .agg(
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.round(F.avg("balance_bucket_avg_3"), 2).alias("avg_balance_bucket_avg_3"),
        F.avg(F.when(F.col("status") == "FROZEN", F.col("balance_avg_3"))).alias("frozen_avg_balance_avg_3"),
    )
)

result = (
    summary
    .select("next_balance_whole_tier", "avg_balance_bucket_avg_3")
    .orderBy(
        F.col("avg_balance_bucket_avg_3").asc_nulls_first(),
        "next_balance_whole_tier",
    )
    .limit(10)
)
