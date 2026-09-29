from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") >= 93000) & F.col("full_name").like("G%"))
    .select(
        "email",
        "salary",
        F.trim("level").alias("clean_level"),
        F.date_sub("hire_date", 75).alias("hire_date_minus_75d"),
    )
)
