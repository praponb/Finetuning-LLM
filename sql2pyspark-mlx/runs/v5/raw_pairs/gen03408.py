from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Stark")
    .select("id", "product_name")
    .unionByName(df_products.filter(F.col("price") > 460).select("id", "product_name"))
)
