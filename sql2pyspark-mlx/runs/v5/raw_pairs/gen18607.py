from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title").isin(["Director", "Designer"]) & (F.col("level") == "L2")) | F.col("dept_id").isin([14, 27, 29, 35]))
    .select(
        "id",
        "job_title",
        "full_name",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
