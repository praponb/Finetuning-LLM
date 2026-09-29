from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("job_title", "level")
    .subtract(
        df_employees
        .filter(F.col("is_remote"))
        .select("job_title", "level")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("WEEK", "hire_date").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("salary_per_l1"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
