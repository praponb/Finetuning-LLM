from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("source"), "hire_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select(F.lit("subscription").alias("source"), F.col("end_date").alias("hire_date"))
    )
    .distinct()
    .orderBy("source")
)
