from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select("category")
    .unionByName(df_products.filter(F.col("supplier_id") == 10).select("category"))
    .orderBy("category")
)
