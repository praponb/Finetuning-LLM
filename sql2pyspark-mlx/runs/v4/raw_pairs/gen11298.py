from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 63)
    .select("id", "age", "country", "segment")
)
