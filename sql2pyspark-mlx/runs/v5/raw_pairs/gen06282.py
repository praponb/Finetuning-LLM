from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

kept_keys = (
    df_employees
    .select("level", "dept_id")
    .subtract(
        df_employees
        .filter(F.upper(F.trim("email")) == "HENRY@STARTUP.IO")
        .select("level", "dept_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.when(F.col("job_title").rlike("^(Analyst|Intern)$"), "EXTENDED").otherwise("PRIORITY").alias("job_title_zone"),
        F.col("min_employee_salary"),
    )
)
