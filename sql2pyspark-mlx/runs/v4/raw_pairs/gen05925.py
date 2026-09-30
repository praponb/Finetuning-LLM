from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .select("id", "category")
    .unionByName(df_products.filter(F.col("price") < 310).select("id", "category"))
)
