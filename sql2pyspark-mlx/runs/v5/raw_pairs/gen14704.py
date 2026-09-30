from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("amount_cume_dist", "id")
w5 = Window.partitionBy("txn_type")
w6 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount_bucket").desc_nulls_first())
w7 = Window.partitionBy("merchant").orderBy(F.col("amount_bucket").asc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-07-20 15:00:00").cast("timestamp"))
    .withColumn("merchant_txn_type_key", F.concat_ws(",", "merchant", "txn_type"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

scored = (
    cleaned
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "merchant_txn_type_key",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.avg("amount").over(w3).alias("amount_avg_2"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "txn_type", "merchant", "merchant_txn_type_key", "amount_bucket")
)

scored_transactions = (
    latest
    .withColumn("amount_bucket_share", F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w5)), 4))
    .withColumn("amount_bucket_rank", F.rank().over(w6))
)

latest_transactions = (
    scored_transactions
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_transactions
    .groupBy("merchant", "txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.max("amount_bucket").alias("max_amount_bucket"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("amount_bucket_rank"))).alias("debit_avg_amount_bucket_rank"),
        F.percentile_approx("amount_bucket", [0.25, 0.5, 0.75]).alias("amount_bucket_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("credit_count").desc())
)
