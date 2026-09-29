from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "hire_date",
        F.initcap(F.trim("level")).alias("level_title"),
        F.concat_ws("/", "job_title", "full_name").alias("job_title_full_name_key"),
    )
    .orderBy("level_title")
)
