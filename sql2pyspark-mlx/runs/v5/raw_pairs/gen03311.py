from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > "2024-05-26") | F.col("manager_id").isNotNull())
)

summary = (
    base
    .groupBy("level", "job_title")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    summary
    .select("level", "job_title")
    .orderBy("level")
)
