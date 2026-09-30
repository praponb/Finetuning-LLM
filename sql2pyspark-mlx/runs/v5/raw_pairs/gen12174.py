from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 200)
    .select("id", "price", "brand", "category")
    .orderBy(F.col("category").desc(), "id")
    .limit(10)
)
