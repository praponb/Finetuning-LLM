from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

matched_keys = (
    df_employees
    .select("dept_id", "level")
    .exceptAll(
        df_employees
        .filter(F.col("hire_date") >= "2025-07-01")
        .select("dept_id", "level")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("sum_employee_salary"),
    )
)
