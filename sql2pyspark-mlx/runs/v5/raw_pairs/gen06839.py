from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.lower(F.trim("full_name")) == "emma brown"))
    .select("*", F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"))
)

enriched = (
    valid
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.*",
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.level").alias("employee_level"),
        F.col("p.hire_date").alias("employee_hire_date_2"),
    )
)

with_dims_employees = (
    with_dims
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.is_remote",
        "e.level",
        "e.employee_hire_date",
        F.col("p2.salary").alias("employee_salary"),
        F.col("p2.hire_date").alias("employee_hire_date_3"),
        F.col("p2.level").alias("employee_level_2"),
    )
)

valid_employees = (
    with_dims_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "employee_hire_date",
        "employee_salary",
        "employee_level_2",
        (F.col("employee_salary") - F.col("salary")).alias("employee_salary_minus_salary"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

result = (
    valid_employees
    .groupBy(
        F.when(F.col("employee_salary") >= 133000, "LARGE").otherwise("SMALL").alias("employee_salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows_2"),
    )
)
