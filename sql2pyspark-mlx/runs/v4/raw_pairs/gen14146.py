from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

result = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))
