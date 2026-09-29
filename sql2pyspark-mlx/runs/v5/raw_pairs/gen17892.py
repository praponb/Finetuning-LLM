from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("dept_id")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("dept_id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "dept_id",
        F.when(F.col("salary") >= 151000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
        F.date_trunc("year", "hire_date").alias("year_start"),
    )
    .agg(
        F.max(F.when(F.col("job_title") == "Analyst", F.col("salary"))).alias("analyst_max_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary"))).alias("l1_total_salary"),
    )
    .orderBy("salary_tier")
)
