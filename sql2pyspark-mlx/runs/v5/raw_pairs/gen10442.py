from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .rollup(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("session_no")).otherwise(0)).alias("fee_session_no"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("amount_stddev").desc_nulls_first())
)
