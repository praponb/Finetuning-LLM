from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.col("email").isNotNull() & (F.col("dept_id") == 38))

summary = (
    valid_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    summary
    .select("hire_month", "latest_hire_date")
    .orderBy(F.col("hire_month").desc(), "latest_hire_date")
)
