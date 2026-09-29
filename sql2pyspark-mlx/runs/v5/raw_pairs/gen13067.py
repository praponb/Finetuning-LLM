from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("txn_type", "merchant")
w_ordered = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_type_sum_amount", "id")
w6 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount_percent_rank_filled").desc_nulls_first(), "id")
w7 = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")

scored = (
    df_transactions
    .withColumn("amount_percent_rank", F.percent_rank().over(w))
    .withColumn("amount_bucket", F.ntile(10).over(w2))
    .withColumn("txn_type_sum_amount", F.sum("amount").over(w3))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter((~F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("txn_time") >= F.to_timestamp(F.lit("2023-10-15 12:00:00"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "amount_percent_rank",
        "amount_bucket",
        "txn_type_sum_amount",
        "row_num",
        F.coalesce("amount_percent_rank", F.lit(0)).alias("amount_percent_rank_filled"),
        F.concat_ws("/", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)

ranked_transactions = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "amount_bucket",
        "merchant_txn_type_key",
    )
)

ranked_transactions_2 = (
    ranked_transactions
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_transactions_2
    .groupBy(
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list_2"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.percentile_approx("amount_bucket", [0.25, 0.5, 0.75]).alias("amount_bucket_quartiles"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("debit_events") < 1000)
    .orderBy(F.col("amount_p95").desc_nulls_last())
)
