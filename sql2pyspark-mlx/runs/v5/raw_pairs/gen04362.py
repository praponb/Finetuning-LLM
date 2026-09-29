from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "level")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") > 119000)
)
