from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-12-27"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy("browser")
    .agg(
        F.max("is_mobile").alias("max_is_mobile"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("mobile_count")
)
