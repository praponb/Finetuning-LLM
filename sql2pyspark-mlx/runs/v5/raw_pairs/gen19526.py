from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        "salary",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.when(F.col("salary") >= 149000, "High").when(F.col("salary") >= 55000, "Medium").otherwise("Low").alias("salary_level"),
    )
    .orderBy("id", F.col("salary").desc())
)
