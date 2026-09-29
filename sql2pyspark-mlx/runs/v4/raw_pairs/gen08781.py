from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
        F.min("salary").alias("min_salary"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") < 482000)
    .drop("total_salary")
    .orderBy(F.col("hire_year").desc())
)
