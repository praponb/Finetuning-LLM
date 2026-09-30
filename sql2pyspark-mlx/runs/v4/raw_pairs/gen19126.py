from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.count("*").alias("row_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") >= 128000)
    .drop("max_salary")
)
