from pyspark.sql import functions as F

base = df_employees.filter(F.col("level") != "L5")

employees_stats = (
    base
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("salary_range") >= 68)
    .select("hire_month", "salary_range", "l1_salary")
    .orderBy(F.col("l1_salary").desc(), F.col("salary_range").desc())
)
