from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("is_new_session")).otherwise(0)).alias("starbucks_is_new_session"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list_2"),
        F.avg("amount").alias("avg_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list_3"),
    )
)
