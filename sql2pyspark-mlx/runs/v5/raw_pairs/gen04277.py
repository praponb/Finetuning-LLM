from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

remaining_keys = (
    df_employees
    .select("level")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L2", "L5", "L3"]))
        .select("level")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
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
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L1|L4)$"), "EXTENDED").when(F.col("level").rlike("^(L2|L3)$"), "GROUP_B").otherwise("CORE").alias("level_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("min_employee_salary").asc_nulls_first())
)
