from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & F.col("is_remote"))
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.upper("job_title").alias("job_title_upper"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)
