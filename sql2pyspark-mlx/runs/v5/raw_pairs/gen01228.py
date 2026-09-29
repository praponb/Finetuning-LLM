from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_stats.job_title_values"),
    )
    .orderBy(F.col("id").desc(), "job_title_values")
)
