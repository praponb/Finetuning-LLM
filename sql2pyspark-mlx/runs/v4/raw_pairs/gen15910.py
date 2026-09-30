from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"), "country")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
    )
    .orderBy(F.col("year_start").asc_nulls_first())
)
