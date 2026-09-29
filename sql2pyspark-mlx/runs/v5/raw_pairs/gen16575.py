from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("email").like("%@outlook.com"))
        .select("id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("salary") >= 160000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary_2"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
    )
    .orderBy(F.col("salary_p50").desc_nulls_last())
)
