from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= "2025-05-30"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .filter(F.col("p.budget").between(1277000, 2861000))
    .select("e.*", F.col("p.location"), F.col("p.dept_name"), F.col("p.budget"))
)

joined = (
    enriched
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.dept_id",
        "s.salary",
        "s.is_remote",
        "s.job_title_filled",
        "s.location",
        "s.dept_name",
        "s.budget",
        F.col("b.budget").alias("department_budget"),
        F.col("b.dept_name").alias("department_dept_name"),
        F.col("b.location").alias("department_location"),
    )
)

base = (
    joined
    .filter(F.col("location").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("Engineering"))))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("budget_scaled", F.round((F.col("budget") * 1.1), 2))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

valid = (
    base
    .filter(F.col("department_location").isNotNull())
    .withColumn("greatest_department_budget_salary", F.greatest("department_budget", "salary"))
)

daily = (
    valid
    .groupBy(
        "dept_id",
        F.when(F.col("department_budget") >= 4020000, "LARGE").otherwise("SMALL").alias("department_budget_tier"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("department_dept_name") == "Sales", 1)).alias("sales_rows"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
    )
    .filter(F.col("salary_p50") > 0)
)

result = (
    daily
    .select(
        "dept_id",
        "department_budget_tier",
        "location",
        "salary_p50",
        "distinct_dept_names",
        F.when(F.col("location").rlike("^(London|Bangkok)$"), "EXTENDED").otherwise("GROUP_B").alias("location_zone"),
    )
    .orderBy(F.col("dept_id").desc())
)
