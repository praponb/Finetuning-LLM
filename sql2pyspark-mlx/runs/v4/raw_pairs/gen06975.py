from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "inner")
    .filter(F.col("department_summary.manager_salary") >= 80)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("department_summary.manager_salary"),
    )
    .orderBy(F.col("manager_salary").desc())
)
