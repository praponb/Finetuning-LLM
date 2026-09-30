from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") > 713000)
    .orderBy(F.col("total_salary").desc())
)
