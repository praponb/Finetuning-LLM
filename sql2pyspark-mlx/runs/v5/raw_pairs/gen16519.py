from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "manager_id",
        "hire_date",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
