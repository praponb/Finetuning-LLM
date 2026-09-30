from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "*",
        F.col("salary").cast("int").alias("salary_int"),
        F.length("level").alias("level_length"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.round(F.avg("level_length"), 2).alias("avg_level_length"),
    )
    .orderBy(F.col("manager_rows").asc_nulls_first())
)
