from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("start_date")
    .subtract(df_employees.select("hire_date"))
)
