from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_type_length").asc_nulls_last(), "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("merchant_length").desc_nulls_first(), "id")
w3 = Window.orderBy("latest_rank", "id")
w4 = Window.partitionBy("txn_type", "merchant")
w5 = Window.partitionBy("txn_type").orderBy("merchant_length", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("merchant", "txn_type")
w7 = Window.partitionBy("txn_type").orderBy(F.col("merchant_length_avg_2").desc(), "id")
w8 = Window.partitionBy("merchant", "txn_type").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("amount").between(2400, 2700))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "txn_type", "merchant")
)

cleaned = (
    ranked
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.coalesce("merchant", F.lit("unassigned")).alias("merchant_filled"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled_2"),
    )
)

cleaned_transactions = (
    cleaned
    .filter(~F.col("txn_type").isin(["FEE", "CREDIT"]))
    .select("*", F.length("merchant").alias("merchant_length"))
)

ranked_transactions = (
    cleaned_transactions
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked_transactions
    .select(
        "*",
        F.lead("merchant_length", 1).over(w3).alias("next_merchant_length"),
        F.count("merchant_length").over(w4).alias("txn_type_count_merchant_length"),
        F.avg("merchant_length").over(w5).alias("merchant_length_avg_2"),
        F.stddev_samp("merchant_length").over(w6).alias("merchant_stddev_samp_merchant_length"),
    )
)

metrics_transactions = (
    metrics
    .filter(F.col("txn_type_count_merchant_length") != 5)
    .withColumn("merchant_length_avg_2_bucket", F.ntile(5).over(w7))
    .withColumn("last_known_next_merchant_length", F.last("next_merchant_length", ignorenulls=True).over(w8))
)

result = (
    metrics_transactions
    .select(
        "id",
        "txn_type",
        "merchant_filled",
        "merchant_length",
        "latest_rank",
        "txn_type_count_merchant_length",
        "merchant_length_avg_2",
        "merchant_stddev_samp_merchant_length",
        "last_known_next_merchant_length",
    )
)
