from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("salary").alias("max_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "left")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("department_totals.max_salary"),
        F.col("department_totals.min_hire_date"),
    )
    .orderBy("max_salary", "budget")
)
