from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.count("*").alias("num_employees"))
    .orderBy("num_employees")
)
