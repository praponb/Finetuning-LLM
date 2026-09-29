from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-05-21").cast("date")) & F.col("level").isNotNull())
    .withColumn("job_title_email_key", F.concat_ws("-", "job_title", "email"))
    .withColumn("full_name_prefix", F.substring("full_name", 1, 4))
    .withColumn("job_title_clean", F.coalesce("job_title", F.lit("NONE")))
)

result = (
    valid
    .select("id", "dept_id")
    .exceptAll(
        df_employees
        .filter(F.col("is_remote"))
        .select("id", "dept_id")
    )
)
