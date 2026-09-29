from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 13)
    .select("hire_date")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 19)
        .select(F.col("opened_date").alias("hire_date"))
    )
)
