from pyspark.sql import functions as F

min_price = df_products.agg(F.min("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") == min_price)
    .select("id", "price")
)
