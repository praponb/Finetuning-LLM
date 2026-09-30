from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Manager", "Analyst"]), "core").otherwise("other").alias("job_title_group"),
        "is_remote",
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .filter(F.col("min_salary") >= 86000)
    .select("job_title_group", "is_remote", "min_salary", "level_list")
)
