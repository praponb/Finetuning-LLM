from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN"]))
    .select(F.lit("account").alias("kind"), "opened_date")
    .union(
        df_employees
        .filter(F.col("salary").between(94000, 130000))
        .select(F.lit("employee").alias("kind"), "hire_date")
    )
    .distinct()
)
