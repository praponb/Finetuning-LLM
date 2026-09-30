from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc())

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-07-18"))) & F.col("full_name").like("I%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws(",", "full_name", "job_title").alias("full_name_job_title_key"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
