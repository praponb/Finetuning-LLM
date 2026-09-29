from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("level") == "L1")
    .withColumn("salary_int", F.col("salary").cast("int"))
)

summary = (
    base
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.avg("salary_int").alias("avg_salary_int"))
)

result = summary.select("hire_month", "avg_salary_int")
