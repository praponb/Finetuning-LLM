from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("level")
    .agg(F.max("salary").alias("max_salary"))
    .filter(F.col("max_salary") > 107000)
    .select("level", "max_salary")
    .orderBy("level")
)
