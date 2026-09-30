from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 125000)
    .select(
        "id",
        "email",
        "full_name",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
    .orderBy("email")
)
