from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

remaining_keys = (
    df_employees
    .select("manager_id", "job_title")
    .subtract(
        df_employees
        .filter(F.month("hire_date").isin([1, 5, 7]))
        .select("manager_id", "job_title")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.col("sum_employee_salary"),
    )
)
