from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "job_title",
        "full_name",
        F.col("salary").cast("int").alias("salary_int"),
    )
)
