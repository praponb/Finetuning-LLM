from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("dedup_rank", "id")
w4 = Window.orderBy(F.col("balance").desc_nulls_last())
w5 = Window.partitionBy("status").orderBy("first_known_balance", "id").rowsBetween(-5, -1)
w6 = Window.orderBy("first_known_balance", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("status").orderBy("first_known_balance", "id")
w8 = Window.partitionBy("status").orderBy(F.col("balance_dense_rank").asc_nulls_first(), "id")

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w2))
    .withColumn("prev_balance", F.lag("balance", 1, 0).over(w3))
    .withColumn("balance_dense_rank", F.dense_rank().over(w4))
)

cleaned = (
    scored
    .filter((F.col("opened_date") >= F.lit("2023-07-12").cast("date")) & (F.col("dedup_rank") <= 1))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "first_known_balance",
        "prev_balance",
        "balance_dense_rank",
        F.round((F.col("dedup_rank") / F.col("balance_dense_rank")), 2).alias("dedup_rank_per_balance_dense_rank"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    not_flagged
    .withColumn("balance_dense_rank_avg_5_prior", F.avg("balance_dense_rank").over(w5))
    .withColumn("prev_balance_avg_3_prior", F.avg("prev_balance").over(w6))
    .withColumn("first_known_balance_bucket", F.ntile(5).over(w7))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy("customer_id")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.sum("first_known_balance_bucket"))
    .select(
        "customer_id",
        F.col("FROZEN").alias("frozen"),
        F.col("CLOSED").alias("closed"),
    )
)
