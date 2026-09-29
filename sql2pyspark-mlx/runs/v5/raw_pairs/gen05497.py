from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & (F.col("level") != "L2"))
    .select(
        "id",
        "job_title",
        F.length("full_name").alias("full_name_length"),
        F.concat("job_title", F.lit(", "), "level").alias("job_title_level"),
    )
    .orderBy(F.col("job_title_level").desc())
)
