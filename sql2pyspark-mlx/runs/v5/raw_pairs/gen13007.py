from pyspark.sql import functions as F

by_dimension = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["dept_name", "job_title"], ["dept_name"]],
        F.col("d.dept_name").alias("dept_name"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.salary")).alias("max_salary"),
        F.grouping_id().alias("gid"),
    )
)

result = by_dimension.select("dept_name", "job_title", "cnt", "gid")
