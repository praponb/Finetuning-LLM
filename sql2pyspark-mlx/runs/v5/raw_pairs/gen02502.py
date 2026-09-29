from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2025-02-01")
    .withColumn("salary_int", F.col("salary").cast("int"))
    .withColumn("salary_int_2", F.col("salary").cast("int"))
    .orderBy(F.col("email").desc(), F.col("level").desc(), "id")
    .limit(10)
)
