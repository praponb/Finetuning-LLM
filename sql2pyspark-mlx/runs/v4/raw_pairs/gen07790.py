from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("account_id").orderBy("row_num", "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-11").cast("date"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
    .withColumn("txn_type_prefix", F.substring("txn_type", 1, 3))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

base = (
    latest
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-08-03 22:00:00"))) & (F.col("txn_type") == "CREDIT"))
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        "txn_type_prefix",
        "resolved_merchant",
        "row_num",
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

enriched = (
    base
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w3))
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        "merchant",
    )
    .agg(
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = summary.select("row_seq_tier", "merchant", "shell_events", "merchant_list")
