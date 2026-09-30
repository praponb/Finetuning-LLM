from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .filter(F.col("p.level") == "L4")
    .select(
        "a.id",
        "a.full_name",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.level",
        F.col("p.salary").alias("employee_salary"),
        F.col("p.level").alias("employee_level"),
    )
)

base = (
    enriched
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "level",
        "employee_salary",
        "employee_level",
        F.substring("job_title", 1, 2).alias("job_title_prefix"),
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.manager_id",
        "a.job_title",
        "a.job_title_prefix",
        F.col("p.email"),
    )
)

enriched_employees = (
    joined
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.manager_id",
        "a.job_title",
        "a.email",
        F.col("d.email").alias("employee_email"),
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.salary"),
    )
)

filtered = (
    enriched_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "employee_full_name",
        "salary",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)

result = (
    filtered
    .withColumn("salary_tier", F.when(F.col("salary") >= 139000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["salary_tier", "job_title"], ["salary_tier"], []],
        "salary_tier",
        "job_title",
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.grouping("salary_tier").alias("grouping_id"),
    )
    .orderBy(F.col("job_title").asc_nulls_last())
)
