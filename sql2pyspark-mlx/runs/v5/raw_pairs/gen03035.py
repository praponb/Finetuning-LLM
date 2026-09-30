from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("amount") >= 3400)
    .select(
        "*",
        F.to_date("txn_time").alias("txn_time_date"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_transactions
    .groupBy(F.date_trunc("YEAR", "txn_time_date").alias("year_start"))
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("burst_id_per_debit"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("burst_id"))).alias("7_eleven_total_burst_id"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("is_new_burst"))).alias("debit_avg_is_new_burst"),
    )
    .orderBy(F.col("lazada_events").asc_nulls_last())
)
