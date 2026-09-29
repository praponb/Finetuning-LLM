from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.stddev_salary") > 31)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("t.stddev_salary"),
        F.col("t.job_title_values"),
    )
    .orderBy("job_title_values", "id")
    .limit(5)
)
