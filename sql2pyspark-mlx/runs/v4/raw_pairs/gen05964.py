from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE", "DEBIT"]))
    .withColumn("txn_time_year", F.year("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.percentile_approx("new_group_flag", 0.9).alias("new_group_flag_p90"),
        F.avg("txn_time_year").alias("avg_txn_time_year"),
        F.percentile_approx("txn_time_year", 0.5).alias("txn_time_year_p50"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_rows"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_rows"),
    )
    .orderBy(F.col("avg_txn_time_year").desc_nulls_last())
)
