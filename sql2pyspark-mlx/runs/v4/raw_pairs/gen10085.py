from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-23").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.coalesce("level", F.lit("Unknown")).alias("level_filled"),
        F.round((F.col("salary") * 0.85), 2).alias("salary_scaled"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

matched_keys = (
    cleaned
    .select("manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("salary") >= 144000)
        .select("manager_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.level"),
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.job_title").alias("employee_job_title"),
    )
)

valid = (
    with_dims
    .filter(F.col("level").isNotNull())
    .select(
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "level_filled",
        "salary_scaled",
        "level",
        "employee_full_name",
        "employee_job_title",
        F.when(F.col("salary_scaled") >= 68, "High").when(F.col("salary_scaled") >= 11, "Medium").otherwise("Low").alias("salary_scaled_level"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

result = (
    valid
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary_scaled",
        "level",
        "employee_full_name",
        "employee_job_title",
        "salary_scaled_level",
        F.when(F.col("job_title").rlike("^(Designer|Director)$"), "EXTENDED").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "OTHER_ZONE").otherwise("CORE").alias("job_title_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("employee_job_title").asc_nulls_first(), F.col("job_title").desc())
)
