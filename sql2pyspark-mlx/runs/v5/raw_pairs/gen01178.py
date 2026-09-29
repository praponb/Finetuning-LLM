from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .pivot("city", ["Tokyo", "Bangkok"])
    .agg(F.sum("age"))
)
