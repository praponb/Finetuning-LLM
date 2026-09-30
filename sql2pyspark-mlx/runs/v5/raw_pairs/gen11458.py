from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance") | F.array_contains("tags", "bestseller"))
)
