from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNull())
    .select(
        "id",
        "hire_date",
        "is_remote",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)
