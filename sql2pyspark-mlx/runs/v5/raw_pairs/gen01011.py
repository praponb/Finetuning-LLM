from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("name").like("%AVI%"))
    .select("city", "age")
    .unionByName(df_customers.filter(F.col("age") <= 59).select("city", "age"))
    .distinct()
    .orderBy("city")
)
