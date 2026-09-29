from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("dept_id", "job_title")
    .exceptAll(
        df_employees
        .filter(F.col("dept_id") == 9)
        .select("dept_id", "job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary"))).alias("designer_total_salary"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .orderBy(F.col("designer_total_salary").desc())
)
