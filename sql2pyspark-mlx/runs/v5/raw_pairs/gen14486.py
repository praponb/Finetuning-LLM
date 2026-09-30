from pyspark.sql import functions as F

result = (
    df_accounts
    .select(F.lit("account").alias("origin"), F.col("opened_date").alias("event_date"))
    .unionByName(
        df_employees
        .select(F.lit("employee").alias("origin"), F.col("hire_date").alias("event_date"))
    )
    .distinct()
)
