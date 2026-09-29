from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("job_title", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("salary") >= 124000)
        .select("job_title", "manager_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("salary") >= 131000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
    .filter(F.col("engineer_salary") >= 0)
    .orderBy(F.col("engineer_salary").asc_nulls_first())
)
