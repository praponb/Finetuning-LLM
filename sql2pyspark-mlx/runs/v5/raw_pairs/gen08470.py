from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-10-06", "2024-03-01"))
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
    .orderBy(F.col("city").desc())
)
