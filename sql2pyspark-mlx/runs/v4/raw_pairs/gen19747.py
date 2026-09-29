from pyspark.sql import functions as F

avg_price = df_products.filter(F.col("category").isin(["Home", "Sports"])).agg(F.avg("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") > avg_price)
    .select("id", "price", "cost")
)
