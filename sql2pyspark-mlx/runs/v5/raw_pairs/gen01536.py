from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("department_totals.job_title_list"),
        F.col("department_totals.avg_salary"),
    )
)
