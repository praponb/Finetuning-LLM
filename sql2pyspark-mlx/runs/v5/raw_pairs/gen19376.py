from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") != "Initech") & (F.col("price") <= 920))
    .select("id", "price")
    .orderBy(F.col("id").desc())
    .limit(3)
)
