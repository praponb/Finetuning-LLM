from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .pivot("category", ["Grocery", "Electronics"])
    .agg(F.sum("cost").alias("amount"), F.count(F.lit(1)).alias("n"))
)
