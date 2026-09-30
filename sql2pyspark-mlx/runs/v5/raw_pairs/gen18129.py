from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("customer_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("is_checking", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status", "account_type").orderBy(F.col("is_checking").asc_nulls_last())
w6 = Window.partitionBy("status", "customer_id").orderBy("opened_date_plus_65d", "id")
w7 = Window.partitionBy("status")

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.date_add("opened_date", 65).alias("opened_date_plus_65d"),
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)

with_gaps = (
    with_last
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_accounts = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

metrics = (
    sessions_accounts
    .withColumn("balance_avg_2", F.avg("balance").over(w4))
    .withColumn("is_checking_rank", F.rank().over(w5))
    .withColumn("cumulative_is_new_burst", F.sum("is_new_burst").over(w3))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("status_max_is_checking", F.max("is_checking").over(w7))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("is_checking") >= 63, "LARGE").otherwise("SMALL").alias("is_checking_tier"),
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.max("balance_avg_2").alias("max_balance_avg_2"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
    )
    .orderBy(F.col("avg_row_seq").desc())
)
