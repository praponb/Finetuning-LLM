from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .select("id", "method", "amount", "status", "new_group_flag", "session_no")
    .orderBy(F.col("method").desc_nulls_first())
)
