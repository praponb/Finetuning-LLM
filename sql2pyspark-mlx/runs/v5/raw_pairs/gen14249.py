from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.hire_date",
        F.col("p.hire_date").alias("employee_hire_date"),
        F.col("p.job_title").alias("employee_job_title"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "employee_job_title",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("employee_job_title").desc())
)
