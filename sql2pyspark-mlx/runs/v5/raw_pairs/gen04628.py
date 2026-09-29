from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(450, 520))
    .select("id", "category")
    .subtract(df_products.filter(F.col("cost") <= 370).select("id", "category"))
)
