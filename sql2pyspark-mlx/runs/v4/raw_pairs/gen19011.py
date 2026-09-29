from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", F.month("hire_date").alias("hire_month"))
    .agg(F.sum("salary").alias("total_salary"))
)
