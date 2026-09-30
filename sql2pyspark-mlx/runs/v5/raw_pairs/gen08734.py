from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

matched_keys = (
    df_employees
    .select("level")
    .intersectAll(
        df_employees
        .filter(F.col("is_remote"))
        .select("level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "level",
        F.when(F.col("full_name").rlike("^(Kanya Srisuk|Pim Chaiyaporn)$"), "EXTENDED").otherwise("PRIORITY").alias("full_name_zone"),
        F.col("sum_employee_salary"),
    )
)
