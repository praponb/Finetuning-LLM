from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("origin"))
    .union(df_employees.select("hire_date", F.lit("employee").alias("origin")))
)
