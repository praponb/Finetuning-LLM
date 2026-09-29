from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy("account_id")
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)
