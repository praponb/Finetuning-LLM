from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "session_id",
        F.when(F.col("browser").rlike("^(Edge|Safari)$"), "PRIORITY").otherwise("CORE").alias("browser_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "country")
    .limit(20)
)
