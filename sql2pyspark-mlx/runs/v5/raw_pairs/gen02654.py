from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"))
    .orderBy(F.col("hire_month").desc())
)
