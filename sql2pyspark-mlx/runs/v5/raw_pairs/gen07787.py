from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") > F.to_date(F.lit("2023-05-15"))))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.location") == "New York")
    .select(
        "src.id",
        "src.email",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.is_remote",
        "src.level",
        "src.level_prefix",
        F.col("p.dept_name"),
        F.col("p.budget"),
        F.col("p.location"),
    )
)

filtered_employees = (
    joined
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "level_prefix",
        "budget",
        "location",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
    )
)

with_dims = (
    filtered_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.email",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.level_prefix",
        "src.budget",
        "src.location",
        F.col("d.level").alias("employee_level"),
        F.col("d.salary"),
        F.col("d.email").alias("employee_email"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_departments.alias("dim"), F.col("s.dept_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.location").alias("department_location"),
        F.col("s.employee_level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.budget")).alias("min_budget"),
        F.round(F.sum(F.col("s.salary")), 2).alias("total_salary"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("total_salary").desc())
)
