from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%IM %"))
    .select(
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.col("is_remote").cast("int").alias("is_remote_flag_2"),
    )
    .orderBy(F.col("is_remote_flag").desc())
)
