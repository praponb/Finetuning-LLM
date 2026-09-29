from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .select("id", "order_id", "status", "session_start_flag", "island_id")
    .orderBy(F.col("order_id").desc_nulls_last())
)
