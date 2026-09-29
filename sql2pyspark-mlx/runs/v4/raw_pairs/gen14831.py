from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("merchant", "account_id").orderBy("amount", "id")
w3 = Window.orderBy("amount", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("merchant")
w_hist = Window.partitionBy("txn_type").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("next_amount_2", "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("merchant_stddev_samp_seq_no").desc_nulls_first(), "id")

metrics = (
    df_transactions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.lead("amount", 1, 0).over(w3).alias("next_amount_2"),
        F.avg("amount").over(w4).alias("amount_avg_6_prior"),
    )
)

metrics_transactions = (
    metrics
    .select(
        "*",
        F.stddev_samp("seq_no").over(w5).alias("merchant_stddev_samp_seq_no"),
        F.last("amount_avg_6_prior", ignorenulls=True).over(w_hist).alias("last_known_amount_avg_6_prior"),
        F.lag("next_amount", 1).over(w7).alias("prev_next_amount"),
    )
)

deduped = (
    metrics_transactions
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    deduped
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        "merchant_stddev_samp_seq_no",
    )
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count_2"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    stats
    .select(
        "week_start",
        "merchant_stddev_samp_seq_no",
        "txn_type_list",
        "debit_count",
        "debit_count_2",
        "span_minutes",
        "cnt",
        "first_txn_time",
    )
    .orderBy(F.col("first_txn_time").desc(), F.col("cnt").desc())
)
