from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-09-06").cast("date"))
    .withColumn("email_full_name", F.concat("email", F.lit("/"), "full_name"))
    .withColumn("level_length", F.length("level"))
)

matched_keys = (
    valid
    .select("dept_id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("email").like("%@startup.io"))
        .select("dept_id", "job_title")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.job_title").alias("employee_job_title"),
    )
)

with_dims_employees = (
    with_dims
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "inner")
    .select(
        "s.email",
        "s.dept_id",
        "s.job_title",
        "s.hire_date",
        "s.is_remote",
        "s.level",
        "s.employee_level",
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)

totals = (
    with_dims_employees
    .groupBy(
        F.when(F.col("budget") >= 4087000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles_2"),
        F.sum("budget").alias("total_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)

result = (
    totals
    .select("budget_tier", "budget_quartiles", "budget_quartiles_2", "location_list")
    .orderBy(F.col("budget_tier").asc_nulls_last())
    .limit(10)
)
