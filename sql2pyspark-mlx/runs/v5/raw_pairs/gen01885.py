from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")

latest = (
    df_sessions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["week_start", "customer_id"], ["week_start"]],
        "week_start",
        "customer_id",
    )
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("uk_events") > 1)
)
