from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("salary")

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") > F.lit("2023-05-13").cast("date")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
        F.substring("level", 1, 3).alias("level_prefix"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)
