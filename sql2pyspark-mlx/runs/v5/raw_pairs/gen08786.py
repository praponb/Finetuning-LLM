from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

remaining_keys = (
    df_employees
    .select("id", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 14)
        .select("id", "dept_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") > 3145000)
    .select("a.*", F.col("d.dept_name"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.hire_date",
        "e.is_remote",
        "e.dept_name",
        F.col("d.level").alias("employee_level"),
    )
)

with_dims_employees = (
    with_dims
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.email",
        "s.dept_id",
        "s.hire_date",
        "s.dept_name",
        F.col("d.budget"),
        F.col("d.location"),
    )
)

result = (
    with_dims_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "dept_id",
        "hire_date",
        "dept_name",
        "budget",
        F.when(F.col("dept_name").rlike("^(Sales|HR)$"), "PRIORITY").when(F.col("dept_name").rlike("^(Engineering|Marketing)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("dept_name_zone"),
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_first(), "dept_id")
)
