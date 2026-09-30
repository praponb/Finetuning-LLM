from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
        F.concat_ws("-", "full_name", "job_title").alias("full_name_job_title_key"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select("e.*", F.col("d.level").alias("employee_level"))
)

joined = (
    enriched
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "inner")
    .filter(F.col("b.dept_id").isin([3, 19]))
    .select(
        "a.id",
        "a.email",
        "a.manager_id",
        "a.job_title",
        "a.hire_date",
        "a.is_remote",
        "a.hire_date_label",
        "a.full_name_job_title_key",
        "a.resolved_level",
        F.col("b.hire_date").alias("employee_hire_date"),
    )
)

enriched_employees = (
    joined
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.*",
        F.col("d.level"),
        F.col("d.salary"),
        F.col("d.email").alias("employee_email"),
    )
)

valid_employees = (
    enriched_employees
    .filter(F.col("hire_date") >= F.lit("2023-02-28").cast("date"))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level_2"),
    )
)

result = (
    valid_employees
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.job_title").alias("employee_job_title"),
        F.col("e.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.is_remote_flag")).alias("min_is_remote_flag"),
        F.sum(F.col("e.salary")).alias("total_salary"),
    )
)
