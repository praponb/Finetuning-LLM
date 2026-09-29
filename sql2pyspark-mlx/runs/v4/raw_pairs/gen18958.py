from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

result = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))
