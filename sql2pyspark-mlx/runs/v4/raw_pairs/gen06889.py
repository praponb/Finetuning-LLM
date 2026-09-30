from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("manager_id") == 1))
    .select(
        "salary",
        F.dayofweek("hire_date").alias("hire_date_dayofweek"),
        F.round("salary", 1).alias("salary_rounded"),
    )
)
