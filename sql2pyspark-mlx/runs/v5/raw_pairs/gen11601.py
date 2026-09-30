from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(~F.col("job_title").eqNullSafe(F.lit("Analyst")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.trim("email").alias("clean_email"),
        F.coalesce("full_name", F.lit("N/A")).alias("full_name_filled"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

result = (
    filtered
    .rollup("level", F.date_trunc("WEEK", "hire_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
    )
)
