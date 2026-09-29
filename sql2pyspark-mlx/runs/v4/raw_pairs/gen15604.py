from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "level",
        F.concat_ws("-", "full_name", "level").alias("full_name_level_key"),
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
    )
    .orderBy("level", "id")
    .limit(20)
)
