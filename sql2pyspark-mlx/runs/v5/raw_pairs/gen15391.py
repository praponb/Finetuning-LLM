from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "session_id",
        F.when(F.col("browser").rlike("^(Edge|Chrome)$"), "GROUP_A").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy(F.col("session_id").desc())
)
