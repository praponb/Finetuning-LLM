from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy("job_title_length", "id")
w3 = Window.orderBy("job_title_length", "id")
w_recent = Window.orderBy("hire_date", "id").rowsBetween(-6, -1)
w_part = Window.partitionBy("level").orderBy("job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-01-27").cast("date")) & (F.col("level") == "L4"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.length("job_title").alias("job_title_length"),
        F.lower("job_title").alias("job_title_lower"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w2).alias("job_title_length_bucket"),
        F.ntile(3).over(w3).alias("job_title_length_bucket_2"),
        F.avg("job_title_length").over(w_recent).alias("job_title_length_avg_6_prior"),
        F.sum("job_title_length").over(w_part).alias("running_job_title_length"),
    )
)
