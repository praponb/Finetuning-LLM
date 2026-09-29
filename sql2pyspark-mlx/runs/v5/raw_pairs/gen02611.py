from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser", "customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("browser", "device").orderBy("browser_length_avg_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("running_min_browser_length", "id")
w6 = Window.partitionBy("device").orderBy("running_min_browser_length_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_avg_browser_length", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-30").cast("date")) & (F.col("device") == "mobile"))
    .withColumn("browser_length", F.length("browser"))
    .withColumn("country_clean", F.coalesce("country", F.lit("UNKNOWN")))
)

with_windows = (
    base
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_length",
        F.avg("browser_length").over(w).alias("running_avg_browser_length"),
        F.min("browser_length").over(w).alias("running_min_browser_length"),
        F.max("browser_length").over(w).alias("running_max_browser_length"),
        F.avg("browser_length").over(w2).alias("browser_length_avg_7d"),
    )
)

enriched = (
    with_windows
    .filter(F.col("browser_length") >= 73)
    .withColumn("last_known_running_avg_browser_length", F.last("running_avg_browser_length", ignorenulls=True).over(w3))
    .withColumn("browser_length_avg_7d_avg_30d", F.avg("browser_length_avg_7d").over(w4))
    .withColumn("running_min_browser_length_bucket", F.ntile(4).over(w5))
)

enriched_sessions = (
    enriched
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_length",
        "running_min_browser_length",
        "running_max_browser_length",
        "last_known_running_avg_browser_length",
        "running_min_browser_length_bucket",
        F.max("running_avg_browser_length").over(w6).alias("running_max_running_avg_browser_length"),
        F.sum("running_avg_browser_length").over(w6).alias("running_sum_running_avg_browser_length"),
        F.min("running_avg_browser_length").over(w6).alias("running_min_running_avg_browser_length"),
        F.avg("browser_length").over(w7).alias("browser_length_avg_6"),
    )
)

result = (
    enriched_sessions
    .groupBy("device")
    .pivot("country", ["SG", "US"])
    .agg(F.sum("browser_length_avg_6"))
)
