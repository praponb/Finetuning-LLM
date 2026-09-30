from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(df_stores.filter(F.col("city") == "Chiang Mai").select("city"))
    .orderBy("city")
)
