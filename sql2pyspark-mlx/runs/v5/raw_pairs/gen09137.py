from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_first())
w4 = Window.partitionBy("merchant").orderBy(F.col("dedup_rank").desc(), "id")
w5 = Window.partitionBy("account_id").orderBy("latest_rank", "id")
w6 = Window.partitionBy("account_id", "merchant").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("txn_time", "id")
w8 = Window.partitionBy("account_id").orderBy("txn_time", "id")

enriched = (
    df_transactions
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.sum("amount").over(w2).alias("amount_sum_90d"),
    )
)

cleaned = (
    enriched
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-06-27"))) & ~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
    )
)

base = (
    cleaned
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("merchant_lower", F.lower("merchant"))
)

deduped = (
    base
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

deduped_transactions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "latest_rank",
        F.row_number().over(w5).alias("seq_no"),
        F.last("merchant", ignorenulls=True).over(w6).alias("last_known_merchant"),
        F.row_number().over(w7).alias("row_seq"),
        F.row_number().over(w8).alias("event_seq"),
    )
)
