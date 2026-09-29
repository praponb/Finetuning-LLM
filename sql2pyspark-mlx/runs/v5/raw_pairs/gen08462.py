from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

remaining_keys = (
    df_employees
    .select("job_title")
    .intersect(
        df_employees
        .filter(F.lower("email").like("%enr%"))
        .select("job_title")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("hire_date").desc_nulls_first(), F.col("id").desc())
)
