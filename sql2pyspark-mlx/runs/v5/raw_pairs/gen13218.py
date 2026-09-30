from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("amount_tier", F.when(F.col("amount") >= 3500, "LARGE").otherwise("SMALL"))
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 6, "PREMIUM").otherwise("STANDARD"))
    .cube("amount_tier", "session_id_tier")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types_2"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count("*").alias("cnt"),
        F.grouping("amount_tier").alias("gid"),
    )
    .orderBy(F.col("amount_tier").desc())
)
