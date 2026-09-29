from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | F.lower("email").like("%enr%"))
    .select("level", F.col("salary").cast("int").alias("salary_int"))
    .orderBy(F.col("salary_int").desc())
)
