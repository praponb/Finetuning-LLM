from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-06-01"))) & F.col("job_title").isNotNull())
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

result = (
    cleaned
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["location", "level"], ["location"]],
        F.col("d.location").alias("location"),
        F.col("src.level").alias("level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("src.salary")).alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc_nulls_first())
)
