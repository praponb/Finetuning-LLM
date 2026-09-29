from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("account_id")
    .agg(
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
    )
    .orderBy(F.col("distinct_account_ids").asc_nulls_first())
)
