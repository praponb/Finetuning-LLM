from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 670)
    .select("category")
    .distinct()
    .orderBy("category")
    .limit(20)
)
