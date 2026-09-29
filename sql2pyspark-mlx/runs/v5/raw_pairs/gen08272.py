from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("job_title", "dept_id")
    .intersect(
        df_employees
        .filter(F.length("full_name") < 5)
        .select("job_title", "dept_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
    )
)
