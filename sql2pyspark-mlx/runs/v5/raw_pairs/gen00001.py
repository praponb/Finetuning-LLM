from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= F.lit("2023-04-30").cast("date")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        F.length("full_name").alias("full_name_length"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.dept_name"), F.col("d.location"))
)
