from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .union(df_customers.select("signup_date"))
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
