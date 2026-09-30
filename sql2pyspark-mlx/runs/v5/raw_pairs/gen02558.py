from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "segment", "country", "age")
    .orderBy(F.col("country").desc(), F.col("age").desc())
)
