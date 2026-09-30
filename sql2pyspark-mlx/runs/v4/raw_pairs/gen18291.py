from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com"))
    .select("city")
    .intersect(df_stores.select("city"))
    .orderBy("city")
)
