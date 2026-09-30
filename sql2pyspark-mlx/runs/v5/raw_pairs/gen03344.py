from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(df_stores.select(F.col("opened_date").alias("signup_date")))
    .distinct()
)
