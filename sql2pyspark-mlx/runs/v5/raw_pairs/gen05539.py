from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-23").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("country").isin(["US", "DE"]), "Tier 1").otherwise("Other").alias("country_group"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy("country", "customer_id")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sum(F.when(F.col("device") == "tablet", F.col("session_id")).otherwise(0)).alias("tablet_session_id"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.max("session_id").alias("max_session_id"),
        F.sum(F.when(F.col("country") == "TH", F.col("session_id")).otherwise(0)).alias("th_session_id"),
    )
    .filter(F.col("distinct_countries") < 1000)
    .orderBy(F.col("desktop_events").desc_nulls_last())
)
