from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupingSets(
        [["account_id", "merchant"], ["account_id"]],
        "account_id",
        "merchant",
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.grouping("account_id").alias("grouping_id"),
    )
    .orderBy(F.col("avg_amount").desc_nulls_last())
)
