from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

remaining_keys = (
    df_employees
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(F.col("level") == "L4")
        .select("manager_id", "id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "is_remote",
        "level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("sum_employee_salary").desc_nulls_first(), "dept_id")
)
