from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget").isNull() & F.col("d.location").isin(["Chiang Mai", "Bangkok"]))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.email"),
        F.upper(F.col("e.job_title")).alias("job_title_upper"),
    )
    .orderBy("email")
)
