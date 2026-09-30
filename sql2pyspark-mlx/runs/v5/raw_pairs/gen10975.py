from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

matched_keys = (
    df_employees
    .select("job_title", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("salary") <= 137000)
        .select("job_title", "manager_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.when(F.col("full_name").rlike("^(  Carol Diaz|Grace Kim)$"), "PRIORITY").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_B").otherwise("EXTENDED").alias("full_name_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("full_name_zone").desc_nulls_last(), "job_title")
)
