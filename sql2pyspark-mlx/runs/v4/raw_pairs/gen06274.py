from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.count("*").alias("num_employees"),
        F.max("salary").alias("max_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 52)
    .orderBy("num_employees")
)
