from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment")
    .distinct()
    .crossJoin(df_stores.select("region").distinct())
    .orderBy("segment", "region")
)
