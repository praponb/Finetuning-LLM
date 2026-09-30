from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("job_title", "level")
    .exceptAll(
        df_employees
        .filter(F.col("email").isNotNull())
        .select("job_title", "level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("manager_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.avg("salary").alias("avg_salary_2"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
    )
    .filter(F.col("avg_salary_2") > 0)
    .orderBy(F.col("avg_salary_2").desc())
)
