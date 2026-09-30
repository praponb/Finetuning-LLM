from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("level") == "L5") | (F.length("full_name") > 11)))
    .select(
        "id",
        "job_title",
        "full_name",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy(F.col("full_name").desc(), "id")
    .limit(20)
)
