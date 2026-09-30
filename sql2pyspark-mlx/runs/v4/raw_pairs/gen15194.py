from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3174000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("budget_per_bangkok"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)
