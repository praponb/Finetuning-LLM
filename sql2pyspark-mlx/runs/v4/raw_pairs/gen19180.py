from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-12-13"))) & F.col("manager_id").isNotNull() & F.col("full_name").like("K%"))
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.upper("job_title").alias("job_title_upper"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

result = (
    base
    .groupBy("job_title")
    .pivot("level", ["L5", "L3", "L1"])
    .agg(F.max("salary").alias("value_sum"))
)
