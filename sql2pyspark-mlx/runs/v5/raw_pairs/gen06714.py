from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .pivot("level", ["L1", "L2", "L3", "L5"])
    .agg(F.max("salary"))
)
