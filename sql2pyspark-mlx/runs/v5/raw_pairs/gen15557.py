from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("level") == "L5")
        .select("id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("YEAR", "hire_date").alias("year_start"), "level")
    .agg(
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
