from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .filter(F.col("department_summary.stddev_salary") > 24)
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("department_summary.stddev_salary"),
    )
)
