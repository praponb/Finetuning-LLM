from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["Tokyo", "London", "Berlin", "Singapore"])
    .agg(F.sum("age"))
)
