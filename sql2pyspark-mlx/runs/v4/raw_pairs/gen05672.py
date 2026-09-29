from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .pivot("category", ["Books", "Home", "Fashion"])
    .agg(F.max("cost").alias("total"), F.count(F.lit(1)).alias("n"))
)
