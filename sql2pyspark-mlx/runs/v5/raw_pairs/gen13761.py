from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull() | (F.col("hire_date") >= "2024-06-19"))
    .withColumn("hire_date_quarter", F.quarter("hire_date"))
    .withColumn("salary_int", F.col("salary").cast("int"))
    .orderBy("hire_date", "id")
    .limit(10)
)
