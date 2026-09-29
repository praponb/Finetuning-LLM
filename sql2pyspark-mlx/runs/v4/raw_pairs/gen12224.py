from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select("country")
    .unionByName(df_customers.filter(F.col("city") == "New York").select("country"))
    .distinct()
    .orderBy("country")
)
