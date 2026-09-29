from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter((F.col("d.location") == "Chiang Mai") & (F.col("e.job_title") == "Analyst"))
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("e.dept_id"),
        F.col("e.is_remote").cast("int").alias("is_remote_flag"),
    )
)
