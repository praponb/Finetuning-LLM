from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_count"))
    .orderBy("dept_name")
    .limit(10)
)
