from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "email", "dept_id", F.col("salary").cast("int").alias("salary_int"))
    .orderBy("email", F.col("id").desc())
    .limit(1)
)
