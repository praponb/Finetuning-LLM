from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(~F.col("merchant").isin(["Amazon", "7-Eleven"]))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.when(F.col("amount") >= 3500, "High").when(F.col("amount") >= 1100, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_transactions
    .groupBy("merchant")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.min("txn_time").alias("first_txn_time"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list_2"),
    )
)
