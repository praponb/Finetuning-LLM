from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Ka%") & F.col("dept_id").isin([8, 19, 25]) & F.col("salary").between(75000, 100000))
    .select("id", "manager_id", F.lower("email").alias("email_lower"))
)
