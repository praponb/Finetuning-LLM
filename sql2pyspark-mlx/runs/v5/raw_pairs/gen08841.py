from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("origin"))
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select(F.col("end_date").alias("hire_date"), F.lit("subscription").alias("origin"))
    )
)
