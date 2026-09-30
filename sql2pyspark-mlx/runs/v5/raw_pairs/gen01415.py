from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower("product_name").like("%mar%"))
    .select("category", "brand")
    .distinct()
)
