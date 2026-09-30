from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("adjusted_amount", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("merchant")
w4 = Window.partitionBy("merchant", "account_id").orderBy(F.col("seq_no").desc())
w5 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("merchant_sum_adjusted_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("merchant_sum_adjusted_amount").desc_nulls_first())
w_hist = Window.orderBy("cumulative_merchant_sum_adjusted_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("merchant").orderBy("cumulative_merchant_sum_adjusted_amount_2", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-05-05").cast("date"))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.15), 2))
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("amount_bucket"),
        F.sum("adjusted_amount").over(w3).alias("merchant_sum_adjusted_amount"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "merchant_sum_adjusted_amount",
        F.sum("merchant_sum_adjusted_amount").over(w5).alias("cumulative_merchant_sum_adjusted_amount"),
        F.sum("merchant_sum_adjusted_amount").over(w6).alias("cumulative_merchant_sum_adjusted_amount_2"),
        F.sum("amount_bucket").over(w7).alias("cumulative_amount_bucket"),
    )
)

base = (
    metrics
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.when(F.col("cumulative_amount_bucket") >= 44, "High").when(F.col("cumulative_amount_bucket") >= 16, "Medium").otherwise("Low").alias("cumulative_amount_bucket_level"),
        F.round((F.col("cumulative_merchant_sum_adjusted_amount") * 1.15), 2).alias("cumulative_merchant_sum_adjusted_amount_scaled"),
    )
)

result = (
    base
    .select(
        "id",
        "account_id",
        "merchant",
        "cumulative_amount_bucket",
        F.cume_dist().over(w_ordered).alias("merchant_sum_adjusted_amount_cume_dist"),
        F.sum("cumulative_amount_bucket").over(w_hist).alias("cumulative_cumulative_amount_bucket"),
        F.avg("cumulative_merchant_sum_adjusted_amount").over(w10).alias("cumulative_merchant_sum_adjusted_amount_avg_2"),
    )
)
