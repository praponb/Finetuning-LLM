from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment", "city")
    .distinct()
    .orderBy("city")
)
