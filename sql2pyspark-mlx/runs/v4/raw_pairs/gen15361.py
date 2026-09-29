from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        "browser",
        "customer_id",
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("session_start_flag_per_mobile"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
    )
    .filter(F.col("duration_min") > 0)
)
