from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.is_remote"))
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.job_title")))).alias("job_title_values"),
    )
)
