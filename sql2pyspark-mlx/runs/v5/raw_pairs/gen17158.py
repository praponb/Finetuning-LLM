from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("job_title").eqNullSafe(F.lit("Manager")))
    .withColumn("is_l5", F.when(F.col("level") == "L5", 1).otherwise(0))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("UNKNOWN")))
)

with_dims = (
    base
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_id").isin([17, 23, 24, 37]))
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.hire_date",
        "s.is_remote",
        F.col("d.email").alias("employee_email"),
        F.col("d.job_title").alias("employee_job_title"),
    )
)

matched_keys = (
    with_dims
    .select("id", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("id", "manager_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-25"))) & F.col("employee_job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "is_remote",
        "employee_email",
        "employee_job_title",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)

result = (
    filtered
    .groupBy(F.date_trunc("WEEK", "hire_date_month_end").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list_2"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("employee_job_title") == "Designer", 1)).alias("designer_rows"),
    )
    .orderBy(F.col("distinct_manager_ids").desc_nulls_last())
)
