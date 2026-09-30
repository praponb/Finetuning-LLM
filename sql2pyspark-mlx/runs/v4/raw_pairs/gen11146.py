from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "new_group_flag",
        "session_no",
        F.when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(Grab|Amazon)$"), "GROUP_A").otherwise("EXTENDED").alias("merchant_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "amount")
)
