from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("employee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.employee_count") > 5)
    .select(
        F.col("d.dept_name"),
        F.col("ds.employee_count"),
        F.col("ds.job_title_values"),
    )
    .orderBy(F.col("employee_count").desc())
)
