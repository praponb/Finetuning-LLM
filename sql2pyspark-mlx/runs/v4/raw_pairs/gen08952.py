from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        "is_remote",
        F.month("hire_date").alias("hire_month"),
    )
    .agg(F.max("salary").alias("max_salary"))
)
