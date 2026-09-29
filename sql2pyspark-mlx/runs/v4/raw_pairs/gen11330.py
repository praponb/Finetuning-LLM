from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("source"))
    .unionByName(
        df_customers
        .filter(F.lower("name").like("%rac%"))
        .select(F.col("signup_date").alias("event_date"), F.lit("customer").alias("source"))
    )
    .distinct()
)
