from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

joined = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.hire_date",
        "e.level",
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.level").alias("employee_level"),
    )
)

result = (
    joined
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        "employee_full_name",
        "employee_level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("level").asc_nulls_first())
)
