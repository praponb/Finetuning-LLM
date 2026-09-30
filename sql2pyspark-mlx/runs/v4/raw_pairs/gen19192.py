from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-05-21 10:00:00"))) & (F.col("country") == "JP"))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.concat("browser", F.lit(" - "), "device").alias("browser_device"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
)
