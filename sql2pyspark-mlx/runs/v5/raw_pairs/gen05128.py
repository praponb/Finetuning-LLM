from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([7, 19, 36]))
    .select("hire_date")
    .subtract(df_subscriptions.select(F.col("start_date").alias("hire_date")))
)
