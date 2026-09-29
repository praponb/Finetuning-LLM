from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("departments.dept_name"),
        F.col("t.job_title_list"),
        F.col("t.director_count"),
    )
    .orderBy("director_count", "dept_name")
)
