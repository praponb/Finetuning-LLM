from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount_per_is_7_eleven", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-02-08").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("is_7_eleven", F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("UNKNOWN")))
)

filtered = (
    cleaned
    .filter(F.col("merchant").isin(["7-Eleven", "Grab"]))
    .withColumn("amount_per_is_7_eleven", (F.col("amount") / F.nullif("is_7_eleven", F.lit(0))))
)

enriched = (
    filtered
    .withColumn("txn_type_max_amount_per_is_7_eleven", F.max("amount_per_is_7_eleven").over(w))
    .withColumn("amount_bucket", F.ntile(10).over(w2))
    .withColumn("amount_max_7d", F.max("amount").over(w3))
    .withColumn("amount_per_is_7_eleven_avg_2", F.avg("amount_per_is_7_eleven").over(w4))
    .withColumn("amount_bucket_2", F.ntile(10).over(w5))
)

cleaned_transactions = (
    enriched
    .filter((F.col("txn_time") >= F.lit("2024-04-18").cast("date")) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

result = (
    cleaned_transactions
    .rollup(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.max("txn_time").alias("last_seen"),
    )
    .filter(F.col("debit_count") > 1)
)
