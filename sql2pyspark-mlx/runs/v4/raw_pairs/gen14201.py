from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "new_group_flag",
        "session_id",
        F.when(F.col("browser").rlike("^(Firefox|Safari)$"), "EXTENDED").when(F.col("browser").rlike("^(Chrome|Edge)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("browser_zone"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
