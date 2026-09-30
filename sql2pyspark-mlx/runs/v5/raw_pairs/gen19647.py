from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Books") & F.col("cost").between(360, 580))
    .agg(F.countDistinct("price").alias("unique_brands"))
)
