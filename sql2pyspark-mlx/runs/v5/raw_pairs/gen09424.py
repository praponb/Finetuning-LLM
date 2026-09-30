from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("job_title", "level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
    .filter(F.col("analyst_count") < 4)
    .select("job_title", "level", "level_list", "analyst_count")
)
