from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "country").orderBy("burst_id", "id")
w4 = Window.partitionBy("device").orderBy("burst_id", "id")
w5 = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-06-13").cast("date"))
    .select("*", F.lower("country").alias("country_lower"))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

enriched = (
    sessions_sessions
    .withColumn("burst_id_bucket", F.ntile(3).over(w3))
    .withColumn("burst_id_bucket_2", F.ntile(10).over(w4))
    .withColumn("burst_id_sum_7d", F.sum("burst_id").over(w5))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "is_new_burst",
        "burst_id",
        "burst_id_bucket",
        "burst_id_bucket_2",
    )
    .orderBy(F.col("burst_id_bucket_2").desc())
)
