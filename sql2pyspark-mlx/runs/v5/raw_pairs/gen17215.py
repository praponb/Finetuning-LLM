from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-27").cast("date")) & F.col("job_title").isin(["Designer", "Manager"]))
    .select(
        "*",
        F.month("hire_date").alias("hire_date_month"),
        F.lower("email").alias("email_lower"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "email_lower",
        "resolved_job_title",
    )
)
