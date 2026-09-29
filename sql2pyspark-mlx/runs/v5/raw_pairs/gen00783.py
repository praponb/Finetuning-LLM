from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("salary_range") >= 49)
    .orderBy(F.col("avg_salary").desc())
)
