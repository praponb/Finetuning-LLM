from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("dept_id", "manager_id")
    .intersect(
        df_employees
        .filter(F.month("hire_date").isin([6, 8, 12]))
        .select("dept_id", "manager_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary_2"),
    )
    .filter(F.col("designer_salary") < 1000000)
)
