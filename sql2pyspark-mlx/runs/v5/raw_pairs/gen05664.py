from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "browser").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy(F.col("running_count_island_id").desc_nulls_first(), "id")
w5 = Window.orderBy("running_max_island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device", "customer_id").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device", "customer_id").orderBy(F.col("running_max_island_id").desc(), "id")

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

enriched = (
    sessions_sessions
    .select(
        "*",
        F.max("island_id").over(w3).alias("running_max_island_id"),
        F.avg("island_id").over(w3).alias("running_avg_island_id"),
        F.count("island_id").over(w3).alias("running_count_island_id"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.city"), F.col("p.credit_limit"))
)

with_windows = (
    joined
    .withColumn("cumulative_island_id", F.sum("island_id").over(w5))
    .withColumn("cumulative_island_id_2", F.sum("island_id").over(w6))
    .withColumn("first_known_city", F.first("city", ignorenulls=True).over(w7))
    .withColumn("cumulative_running_count_island_id", F.sum("running_count_island_id").over(w8))
    .withColumn("running_max_island_id_bucket", F.ntile(5).over(w9))
)

result = (
    with_windows
    .groupBy("browser")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.max(F.when(F.col("browser") == "Firefox", F.col("running_avg_island_id"))).alias("firefox_max_running_avg_island_id"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("distinct_devices").desc_nulls_first())
)
