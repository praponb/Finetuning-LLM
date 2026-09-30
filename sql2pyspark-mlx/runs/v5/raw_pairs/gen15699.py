from pyspark.sql import functions as F

result = (
    df_employees
    .cube(F.month("hire_date").alias("hire_month"), "manager_id")
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.col("hire_month").desc())
)
