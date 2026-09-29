from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

remaining_keys = (
    df_employees
    .select("level")
    .intersectAll(
        df_employees
        .filter(F.lower(F.trim("email")) == "david.lee@gmail.com")
        .select("level")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.col("avg_employee_salary"),
    )
    .orderBy("job_title")
)
