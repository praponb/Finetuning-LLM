from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("is_remote"))
    .withColumn("full_name_lower", F.lower("full_name"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("N/A")))
)

with_dims = (
    valid
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.dept_name") == "Sales")
    .select(
        "e.id",
        "e.dept_id",
        "e.job_title",
        "e.salary",
        "e.level",
        "e.full_name_lower",
        "e.resolved_job_title",
        F.col("p.budget"),
        F.col("p.location"),
        F.col("p.dept_name"),
    )
)

remaining_keys = (
    with_dims
    .select("job_title", "level")
    .intersect(
        df_employees
        .filter(F.col("full_name").like("J%"))
        .select("job_title", "level")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

with_dims_employees = (
    kept_rows
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.salary",
        "src.level",
        "src.full_name_lower",
        "src.resolved_job_title",
        "src.budget",
        "src.location",
        "src.dept_name",
        F.col("b.budget").alias("department_budget"),
        F.col("b.dept_name").alias("department_dept_name"),
    )
)

with_dims_employees_2 = (
    with_dims_employees
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.salary",
        "src.level",
        "src.department_budget",
        "src.department_dept_name",
        F.col("b.budget").alias("department_budget_2"),
        F.col("b.dept_name").alias("department_dept_name_2"),
    )
)

result = (
    with_dims_employees_2
    .alias("src")
    .join(df_departments.alias("dim"), F.col("src.dept_id") == F.col("dim.id"), "left")
    .cube(
        F.col("dim.location").alias("location"),
        F.col("src.department_dept_name_2").alias("department_dept_name_2"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.department_budget_2")), 2).alias("avg_department_budget_2"),
        F.grouping_id().alias("gid"),
    )
)
