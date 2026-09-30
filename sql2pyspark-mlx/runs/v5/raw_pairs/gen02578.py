from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status", "account_type").orderBy("opened_date", "id")
w3 = Window.partitionBy("status", "account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("next_balance", "id")
w5 = Window.partitionBy("customer_id").orderBy("rank_in_group", "id")
w6 = Window.partitionBy("customer_id").orderBy("dedup_rank", "id")
w7 = Window.partitionBy("customer_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        F.avg("balance").over(w).alias("balance_avg_3"),
        F.lead("balance", 1, 0).over(w2).alias("next_balance"),
        F.last("status", ignorenulls=True).over(w3).alias("last_known_status"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

deduped_accounts = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped_accounts
    .select(
        "*",
        F.lead("balance").over(w6).alias("next_balance_2"),
        F.lag("balance").over(w6).alias("prev_balance"),
        F.lead("balance_avg_3").over(w6).alias("next_balance_avg_3"),
        F.lag("balance_avg_3").over(w6).alias("prev_balance_avg_3"),
        F.row_number().over(w6).alias("event_seq"),
        F.sum("next_balance").over(w7).alias("running_next_balance"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("running_next_balance") >= 303052, "LARGE").otherwise("SMALL").alias("running_next_balance_tier"),
    )
    .agg(
        F.max("next_balance").alias("max_next_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)

result = summary.select("running_next_balance_tier", "max_next_balance", "balance_stddev")
