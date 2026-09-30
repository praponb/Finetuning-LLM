from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
        F.floor("salary").alias("salary_whole"),
    )
)

base = (
    valid
    .filter(F.col("salary_whole").between(64, 72))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "job_title_filled",
        "salary_whole",
        F.concat_ws("/", "email", "full_name").alias("email_full_name_key"),
        F.date_sub("hire_date", 30).alias("hire_date_minus_30d"),
    )
)

base_employees = (
    base
    .filter(F.col("level").isNotNull() & F.col("level").isin(["L3", "L5", "L2"]))
    .withColumn("level_filled", F.coalesce("level", F.lit("unassigned")))
    .withColumn("resolved_level", F.coalesce("level", F.lit("DIRECT")))
)

result = (
    base_employees
    .withColumn("salary_whole_tier", F.when(F.col("salary_whole") >= 64, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date_minus_30d"))
    .groupingSets(
        [["salary_whole_tier", "activity_month"], ["salary_whole_tier"]],
        "salary_whole_tier",
        "activity_month",
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.percentile_approx("salary_whole", [0.25, 0.5, 0.75]).alias("salary_whole_quartiles"),
    )
    .orderBy("num_rows")
)
