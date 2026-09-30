from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

kept_keys = (
    df_employees
    .select("job_title", "level")
    .subtract(
        df_employees
        .filter(F.col("full_name").like("%Lee%"))
        .select("job_title", "level")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("max_employee_salary").desc_nulls_last())
)
