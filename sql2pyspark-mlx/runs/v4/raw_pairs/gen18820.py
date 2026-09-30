from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io") & (F.col("age") != 41))
    .select("id", "email")
    .orderBy(F.col("id").desc())
)
