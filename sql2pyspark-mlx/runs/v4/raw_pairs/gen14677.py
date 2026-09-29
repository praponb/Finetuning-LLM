from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.level").alias("employee_level"),
        F.col("p.email").alias("employee_email"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "employee_salary",
        "employee_level",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("employee_salary").desc(), "id")
)
