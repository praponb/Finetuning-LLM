from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 600)
    .select("stock_qty", "brand")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("stock_qty", "brand")
    )
    .orderBy(F.col("brand").desc())
)
