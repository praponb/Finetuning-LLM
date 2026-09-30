from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("hire_date")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") >= 2024)
        .select(F.col("start_date").alias("hire_date"))
    )
    .orderBy("hire_date")
)
