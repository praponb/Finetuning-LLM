from pyspark.sql import functions as F

max_price = df_products.agg(F.max("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= max_price)
    .select("id", "price", "cost")
    .orderBy(F.col("cost").desc())
)
