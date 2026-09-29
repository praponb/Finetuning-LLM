from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .unionByName(df_subscriptions.select(F.col("end_date").alias("hire_date")))
)
