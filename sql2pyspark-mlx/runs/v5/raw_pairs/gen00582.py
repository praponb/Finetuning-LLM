from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("source"))
    .unionByName(
        df_accounts
        .filter(F.col("balance").between(30000, 58000))
        .select(F.col("opened_date").alias("hire_date"), F.lit("account").alias("source"))
    )
    .distinct()
)
