from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isin([10, 15, 27, 35]))
        .select("manager_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("salary") >= 126000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.count("*").alias("cnt"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy("total_salary")
)
