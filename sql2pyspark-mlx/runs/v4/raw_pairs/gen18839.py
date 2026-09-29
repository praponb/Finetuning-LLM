from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%MMA%") & (F.col("manager_id") == 14))
    .select(
        "id",
        "email",
        "dept_id",
        F.date_trunc("YEAR", "hire_date").alias("hire_date_period"),
    )
)
