from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser_length_tier").orderBy(F.col("us_total_session_no").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-09-30 11:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.length("browser").alias("browser_length"),
    )
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("browser_length") >= 67, "HIGH").otherwise("LOW").alias("browser_length_tier"),
        "customer_id",
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.sum(F.when(F.col("country") == "US", F.col("session_no"))).alias("us_total_session_no"),
    )
    .filter(F.col("us_total_session_no") < 0)
    .withColumn("us_total_session_no_rank", F.rank().over(w3))
    .orderBy(F.col("session_start_flag_tier").desc())
)
