from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com"))
    .select(
        "full_name",
        "job_title",
        "email",
        F.lower("full_name").alias("full_name_lower"),
    )
)
