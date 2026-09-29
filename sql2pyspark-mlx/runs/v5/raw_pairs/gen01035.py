from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 9, 25, 26]) & (F.col("salary") <= 63000))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") >= 126000)
    .orderBy("max_salary", F.col("l5_count").desc())
)
