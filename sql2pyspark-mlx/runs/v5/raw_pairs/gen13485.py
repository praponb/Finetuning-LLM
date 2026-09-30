from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(((F.col("hire_date") >= "2025-02-01") & F.col("full_name").like("Dav%")) | F.col("manager_id").isin([3, 4, 39]))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

summary = (
    filtered_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    summary
    .select("hire_month", "first_hire_date")
    .orderBy("hire_month")
)
