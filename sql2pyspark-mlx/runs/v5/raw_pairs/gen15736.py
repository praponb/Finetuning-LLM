from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(103000, 143000) | F.col("is_remote"))
    .select(
        "level",
        "email",
        "job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy("is_remote_flag")
)
