from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy("avg_salary")
)
