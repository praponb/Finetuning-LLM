from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io") & F.col("dept_id").isin([7, 15, 30]))
    .select("id", "manager_id", F.trim("level").alias("clean_level"))
    .orderBy(F.col("manager_id").desc(), F.col("id").desc())
)
