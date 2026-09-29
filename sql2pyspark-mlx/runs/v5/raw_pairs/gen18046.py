from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.col("e.salary")).alias("total_salary"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
    )
    .orderBy(F.col("dept_name").desc(), "id")
    .limit(3)
)
