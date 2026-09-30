from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") <= 50)
    .select("id", "segment")
    .orderBy("id")
)
