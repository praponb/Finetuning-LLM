from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-10 03:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.upper("merchant").alias("merchant_upper"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

enriched = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_new_session",
        F.sum("is_new_session").over(w4).alias("cumulative_is_new_session"),
        F.first("amount", ignorenulls=True).over(w5).alias("first_known_amount"),
    )
)

result = (
    enriched
    .cube(
        F.when(F.col("first_known_amount") >= 3500, "LARGE").otherwise("SMALL").alias("first_known_amount_tier"),
    )
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("merchant")).alias("num_merchants_2"),
    )
    .orderBy(F.col("duration_min").asc_nulls_last())
)
