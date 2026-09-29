from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.to_date(F.lit("2023-05-16")))
    .select(
        "*",
        F.concat("job_title", F.lit(" "), "email").alias("job_title_email"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

result = (
    cleaned
    .cube(
        F.when(F.col("salary") >= 123000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_per_l5"),
    )
    .filter(F.col("salary_per_l5") > 0)
    .orderBy(F.col("salary_per_l5").desc_nulls_first())
)
