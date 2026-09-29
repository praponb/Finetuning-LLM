from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= "2024-08-01")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("country") == "US", F.col("is_uk")).otherwise(0)).alias("us_is_uk"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("is_new_burst_per_us"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("is_new_burst")).otherwise(0)).alias("chrome_is_new_burst"),
    )
    .filter(F.col("sg_count") < 1000)
)
