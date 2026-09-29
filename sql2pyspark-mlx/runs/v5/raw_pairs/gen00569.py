from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("source"), F.col("hire_date").alias("event_date"))
    .unionByName(
        df_accounts
        .select(F.lit("account").alias("source"), F.col("opened_date").alias("event_date"))
    )
    .distinct()
)
