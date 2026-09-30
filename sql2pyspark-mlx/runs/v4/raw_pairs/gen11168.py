from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "job_title")
    .intersect(
        df_employees
        .filter(F.col("full_name").like("%Lee%"))
        .select("id", "job_title")
    )
)
