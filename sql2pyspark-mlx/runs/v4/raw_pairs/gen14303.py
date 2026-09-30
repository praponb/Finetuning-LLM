from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("txn_type", "merchant")
w5 = Window.partitionBy("txn_type").orderBy("new_group_flag", "id")
w6 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("txn_type", "merchant").orderBy("new_group_flag_bucket", "id")
w9 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("txn_type").orderBy(F.col("txn_time_quarter_avg_90d").asc_nulls_last(), "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-25 04:00:00").cast("timestamp")) & (F.col("txn_type") == "CREDIT"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
        F.floor("amount").alias("amount_whole"),
    )
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.avg("amount_whole").over(w3).alias("amount_whole_avg_6_prior"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.ntile(10).over(w5).alias("new_group_flag_bucket"),
    )
)

cleaned = (
    scored
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.quarter("txn_time").alias("txn_time_quarter"),
        F.add_months("txn_time_day", 3).alias("txn_time_day_plus_months"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

metrics = (
    cleaned
    .withColumn("amount_bucket", F.ntile(10).over(w6))
    .withColumn("txn_time_quarter_avg_90d", F.avg("txn_time_quarter").over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("running_amount_share", F.sum("amount_share").over(w9))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .groupBy("merchant")
    .pivot("txn_type", ["FEE", "CREDIT", "REFUND"])
    .agg(F.max("amount").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "merchant",
        F.col("FEE_value_sum").alias("fee_value_sum"),
        F.col("FEE_cnt").alias("fee_cnt"),
        F.col("CREDIT_value_sum").alias("credit_value_sum"),
        F.col("CREDIT_cnt").alias("credit_cnt"),
        F.col("REFUND_value_sum").alias("refund_value_sum"),
        F.col("REFUND_cnt").alias("refund_cnt"),
    )
)
