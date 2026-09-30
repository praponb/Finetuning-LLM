from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "burst_id",
        F.when(F.col("country").rlike("^(TH|US)$"), "GROUP_A").when(F.col("country").rlike("^(SG|JP)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("country_zone"),
    )
)
