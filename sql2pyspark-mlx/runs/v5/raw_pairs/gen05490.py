from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

remaining_keys = (
    df_employees
    .select("manager_id", "dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L3")
        .select("manager_id", "dept_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("job_title").rlike("^(Director|Analyst)$"), "GROUP_B").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "PRIORITY").otherwise("CORE").alias("job_title_zone"),
        F.col("avg_employee_salary"),
    )
)
