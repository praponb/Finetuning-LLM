from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 74000) & ~F.col("level").isin(["L3", "L2", "L4"]))
    .select("id", "job_title", "manager_id", F.length("email").alias("email_length"))
    .orderBy("id", F.col("manager_id").desc())
)
