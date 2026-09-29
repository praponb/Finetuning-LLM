from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy("island_id")
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count("*").alias("event_count"),
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
    )
    .filter(F.col("num_txn_types") >= 1)
    .orderBy(F.col("island_id").desc())
)
