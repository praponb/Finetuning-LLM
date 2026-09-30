from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("burst_id")).otherwise(0)).alias("7_eleven_burst_id"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.sum("burst_id").alias("total_burst_id"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows_2"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("burst_id")).otherwise(0)).alias("credit_burst_id"),
    )
)
