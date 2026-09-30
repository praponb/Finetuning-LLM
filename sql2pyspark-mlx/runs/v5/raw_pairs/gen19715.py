from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "level",
        "full_name",
        F.month("hire_date").alias("hire_date_month"),
    )
)
