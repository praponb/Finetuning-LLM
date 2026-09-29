from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_session_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.size(F.collect_set("merchant")).alias("num_merchants_2"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .orderBy(F.col("num_merchants_2").desc_nulls_first())
)
