from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

result = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "employee_salary",
        "employee_hire_date",
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(5)
)
