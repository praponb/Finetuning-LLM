from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("is_remote", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.max("hire_date").alias("last_hire_date"))
)
