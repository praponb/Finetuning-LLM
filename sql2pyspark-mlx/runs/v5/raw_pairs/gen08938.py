from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .filter(F.col("stddev_salary") >= 62)
    .orderBy(F.col("min_salary").desc(), F.col("hire_month").desc())
)
