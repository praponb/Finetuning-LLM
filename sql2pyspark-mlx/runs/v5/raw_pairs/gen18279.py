from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_employees.alias("b"), F.col("a.manager_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.full_name").alias("employee_full_name"), F.col("b.level"))
)

base = (
    joined
    .filter(F.col("level").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L1"))))
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "email_title",
        "resolved_level",
        "level",
        F.concat("email", F.lit(" "), "employee_full_name").alias("email_employee_full_name"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.hire_date",
        "e.resolved_level",
        "e.level",
        "e.email_employee_full_name",
        "e.job_title_filled",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

filtered_employees = (
    enriched
    .filter(F.col("hire_date") > F.lit("2023-04-25").cast("date"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "hire_date",
        "resolved_level",
        "level",
        "email_employee_full_name",
        "job_title_filled",
        "budget",
        "location",
        "dept_name",
        F.concat_ws("-", "level", "full_name").alias("level_full_name_key"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level_2"),
    )
)

result = (
    filtered_employees
    .rollup(
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
    )
    .orderBy(F.col("l3_count").desc_nulls_last())
)
