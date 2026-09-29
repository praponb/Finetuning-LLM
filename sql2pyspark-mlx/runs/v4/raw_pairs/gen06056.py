from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([29, 32, 35, 36]) & F.col("is_remote"))
    .groupBy("level")
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") >= 820000)
    .orderBy("level")
)
