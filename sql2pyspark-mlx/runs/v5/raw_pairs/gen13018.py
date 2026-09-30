from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .select("id", "country", "session_id")
    .orderBy(F.col("id").desc_nulls_last(), "country")
    .limit(20)
)
