from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("level", "id")
    .subtract(
        df_employees
        .filter(F.col("email").isNull())
        .select("level", "id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("salary") >= 128000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
    .filter(F.col("designer_rows") >= 1)
    .orderBy("salary_tier")
)
