from pyspark.sql import functions as F

base = df_employees.filter(~F.col("is_remote") & F.col("is_remote"))

agg = (
    base
    .groupBy("is_remote")
    .agg(F.min("salary").alias("min_salary"))
)

result = agg.filter(F.col("min_salary") >= 95000)
