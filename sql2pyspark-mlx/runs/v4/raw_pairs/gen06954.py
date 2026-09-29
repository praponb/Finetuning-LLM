from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(4600, 8400))
    .select("city")
    .unionByName(df_customers.filter(F.lower("name").like("%any%")).select("city"))
    .distinct()
    .orderBy("city")
)
