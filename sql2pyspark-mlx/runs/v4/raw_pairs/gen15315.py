from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Fashion", "Electronics"]))
    .select("id", "category")
    .subtract(df_products.filter(F.col("stock_qty") <= 344).select("id", "category"))
)
