from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_id").orderBy("amount", "id")

metrics = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND", "FEE"]))
    .withColumn("prev_amount", F.lag("amount").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.max("txn_time").alias("last_txn_time"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("event_seq_per_fee"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("num_merchants") > 1)
    .orderBy(F.col("quarter_start").desc())
)
