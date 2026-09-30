from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant")
w_grp = Window.partitionBy("txn_type", "account_id").orderBy("merchant_count_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type", "account_id").orderBy("merchant_count_amount", "id")

deduped = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    deduped
    .filter(F.col("txn_time") >= "2025-10-30")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        F.row_number().over(w2).alias("row_seq"),
        F.count("amount").over(w3).alias("amount_count_7d"),
        F.first("merchant", ignorenulls=True).over(w4).alias("first_known_merchant"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
        F.count("amount").over(w5).alias("merchant_count_amount"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount_count_7d",
        "first_known_merchant",
        "merchant_count_amount",
        F.count("amount_count_7d").over(w_grp).alias("running_count_amount_count_7d"),
        F.sum("amount_count_7d").over(w_grp).alias("running_sum_amount_count_7d"),
        F.min("amount_count_7d").over(w_grp).alias("running_min_amount_count_7d"),
        F.max("amount_count_7d").over(w_grp).alias("running_max_amount_count_7d"),
    )
)

deduped_transactions = (
    enriched
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_type",
        "amount_count_7d",
        "first_known_merchant",
        "running_max_amount_count_7d",
    )
)

result = (
    deduped_transactions
    .select(
        "id",
        "txn_type",
        "first_known_merchant",
        F.when(F.col("txn_type").rlike("^(FEE|CREDIT)$"), "PRIORITY").when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
    .orderBy("id")
    .limit(20)
)
