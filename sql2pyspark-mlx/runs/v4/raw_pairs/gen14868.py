from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

result = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))
