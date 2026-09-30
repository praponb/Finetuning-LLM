from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select("city")
    .unionByName(df_customers.filter(F.col("name").isNotNull()).select("city"))
)
