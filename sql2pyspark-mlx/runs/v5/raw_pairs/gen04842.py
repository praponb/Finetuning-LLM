from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type")
w2 = Window.partitionBy("account_id").orderBy("merchant_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("merchant_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("txn_type").orderBy("account_id_stddev_samp_merchant_length")
w_ordered = Window.partitionBy("account_id").orderBy(F.col("merchant_length").desc(), "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-04-21").cast("date")) & F.col("amount").between(1200, 1700))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        F.length("merchant").alias("merchant_length"),
    )
)

scored = (
    valid
    .filter(F.col("is_flagged"))
    .withColumn("account_id_stddev_samp_merchant_length", F.stddev_samp("merchant_length").over(w))
    .withColumn("first_known_merchant_length", F.first("merchant_length", ignorenulls=True).over(w2))
    .withColumn("running_amount", F.sum("amount").over(w3))
)

scored_transactions = (
    scored
    .select(
        "*",
        F.rank().over(w_running).alias("account_id_stddev_samp_merchant_length_rank"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

cleaned = (
    scored_transactions
    .filter(F.col("txn_type") != "CREDIT")
    .select(
        "id",
        "account_id",
        "merchant_length",
        "account_id_stddev_samp_merchant_length",
        "running_amount",
        "account_id_stddev_samp_merchant_length_rank",
        "event_seq",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

filtered = (
    cleaned
    .filter(F.col("account_id_stddev_samp_merchant_length_rank") < 4)
    .select(
        "*",
        F.when(F.col("account_id_stddev_samp_merchant_length") >= 60, "High").when(F.col("account_id_stddev_samp_merchant_length") >= 34, "Medium").otherwise("Low").alias("account_id_stddev_samp_merchant_length_band"),
    )
)

result = (
    filtered
    .select(
        "id",
        "merchant_length",
        "account_id_stddev_samp_merchant_length",
        "event_seq",
        "is_flagged_label",
        "account_id_stddev_samp_merchant_length_band",
    )
    .orderBy(F.col("id").desc())
)
