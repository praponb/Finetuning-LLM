from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("quarter_start").orderBy(F.col("duration_min").desc())

gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"), "txn_type")
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("is_new_session"))).alias("fee_total_is_new_session"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", F.col("amount")).otherwise(0)).alias("7_eleven_amount"),
    )
    .filter(F.col("fee_total_is_new_session") < 1000000)
    .withColumn("duration_min_percent_rank", F.percent_rank().over(w3))
    .orderBy("7_eleven_amount")
)
