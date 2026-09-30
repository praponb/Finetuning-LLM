from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("session_no_per_refund"),
    )
    .orderBy(F.col("new_group_flag_tier").desc_nulls_last())
)
