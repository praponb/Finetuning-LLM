from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-03-12").cast("date")) & ~F.col("level").isin(["L5", "L4"]))
    .select(
        "*",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.salary") > 131000)
    .select(
        "s.id",
        "s.full_name",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.salary",
        "s.hire_date",
        "s.email_slug",
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.email").alias("employee_email"),
    )
)

remaining_keys = (
    joined
    .select("dept_id", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("full_name").like("Oli%"))
        .select("dept_id", "manager_id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.email",
        "src.manager_id",
        "src.job_title",
        "src.email_slug",
        "src.employee_hire_date",
        F.col("e.email").alias("employee_email_2"),
        F.col("e.level"),
    )
)

base = (
    with_dims
    .filter(F.col("email").isNotNull() & F.col("job_title").isNotNull())
    .select(
        "*",
        F.length("full_name").alias("full_name_length"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .groupBy(
        F.col("d.level").alias("employee_level"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.full_name_length")).alias("min_full_name_length"),
    )
)
