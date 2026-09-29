from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("min_salary") < 110000)
    .orderBy(F.col("hire_month").desc(), "stddev_salary")
)
