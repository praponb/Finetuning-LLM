from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") >= 72000) | (F.length("email") > 7))
    .select(
        "id",
        "email",
        "hire_date",
        "job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy(F.col("is_remote_flag").desc())
)
