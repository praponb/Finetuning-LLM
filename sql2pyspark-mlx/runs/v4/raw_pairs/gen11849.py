from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("level")
    .intersectAll(
        df_employees
        .filter(F.col("job_title").isin(["Manager", "Analyst", "Director"]))
        .select("level")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("full_name", "email", "job_title", "hire_date", "is_remote", "level")
    .orderBy(F.col("job_title").desc_nulls_last(), "hire_date")
)
