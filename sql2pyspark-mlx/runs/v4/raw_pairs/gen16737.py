from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-16").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.when(F.col("job_title") == "Manager", 1).otherwise(0).alias("is_manager"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

filtered = (
    base
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-03-29"))) & F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("job_title").isin(["Manager", "Director"]), "Priority").otherwise("Other").alias("job_title_group"),
    )
)

remaining_keys = (
    filtered
    .select("level", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.lower(F.trim("email")) == "alice@gmail.com")
        .select("level", "manager_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

filtered_employees = (
    verified
    .filter((F.col("hire_date") > F.lit("2023-12-22").cast("date")) & F.col("job_title").isNotNull() & (F.col("is_manager") > 39))
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

result = (
    filtered_employees
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "is_manager",
        F.when(F.col("job_title").rlike("^(Designer|Director)$"), "EXTENDED").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("job_title_zone"),
        F.col("min_employee_salary"),
    )
    .orderBy("level", "manager_id")
)
