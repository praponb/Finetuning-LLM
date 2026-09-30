from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-02-19").cast("date")) & (F.col("started_at") >= "2024-04-01"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("Unknown")))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Edge", "Chrome"]), "Group A").otherwise("Other"))
)

result = (
    filtered
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["week_start", "browser"], ["week_start"], []],
        "week_start",
        "browser",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.max("started_at").alias("ended_at"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
        F.count("*").alias("cnt"),
        F.grouping("week_start").alias("grouping_level"),
    )
)
