from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .pivot("level", ["L3", "L5"])
    .agg(F.sum("salary"))
)
