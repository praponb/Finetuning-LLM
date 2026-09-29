from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 5500)
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
)
