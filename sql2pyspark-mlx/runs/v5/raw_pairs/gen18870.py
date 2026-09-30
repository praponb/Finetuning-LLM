from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.hire_date",
        "e.is_remote",
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.full_name").alias("employee_full_name"),
    )
)

result = (
    enriched
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "employee_job_title",
        "employee_full_name",
        F.when(F.col("employee_job_title").rlike("^(Director|Engineer)$"), "GROUP_B").when(F.col("employee_job_title").rlike("^(Analyst|Manager)$"), "PRIORITY").otherwise("EXTENDED").alias("employee_job_title_zone"),
    )
)
