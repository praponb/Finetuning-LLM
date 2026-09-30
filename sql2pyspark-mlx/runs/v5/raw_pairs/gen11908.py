from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "*",
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
        F.concat_ws("-", "level", "email").alias("level_email_key"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled"),
    )
)

result = (
    filtered
    .groupBy("job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary"))).alias("l5_total_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("salary_per_l3"),
    )
)
