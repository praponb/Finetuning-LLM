from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L5")
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
    .orderBy(F.col("salary").desc(), "hire_date_month_end", "id")
    .limit(10)
)
