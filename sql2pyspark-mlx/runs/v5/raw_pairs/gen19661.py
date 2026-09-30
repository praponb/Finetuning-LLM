from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("browser") != "Edge") & (F.col("started_at") > F.lit("2023-09-03").cast("date")))
    .withColumn("country_group", F.when(F.col("country").isin(["US", "JP"]), "Core").otherwise("Other"))
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["week_start", "new_group_flag_tier"], ["week_start"], []],
        "week_start",
        "new_group_flag_tier",
    )
    .agg(
        F.sum(F.when(F.col("browser") == "Edge", F.col("new_group_flag")).otherwise(0)).alias("edge_new_group_flag"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("island_id_per_de"),
        F.count("*").alias("event_count"),
        F.grouping("week_start").alias("gid"),
    )
    .orderBy(F.col("week_start").desc())
)
