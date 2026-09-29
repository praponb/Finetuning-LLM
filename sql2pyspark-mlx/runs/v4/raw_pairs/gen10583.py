from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("E.job_title_list"),
    )
    .orderBy(F.col("dept_name").desc())
)
