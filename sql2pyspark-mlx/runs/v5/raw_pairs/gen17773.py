from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .intersect(df_customers.select("signup_date"))
    .orderBy(F.col("opened_date").desc())
)
