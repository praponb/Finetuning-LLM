from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") > "2023-10-01")
    .groupBy("level")
    .agg(F.min("salary").alias("min_salary"))
    .filter(F.col("min_salary") >= 125000)
)
