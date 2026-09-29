from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 34)
    .select("city")
    .subtract(df_stores.select("city"))
    .orderBy("city")
)
