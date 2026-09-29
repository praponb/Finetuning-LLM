from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

result = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)
