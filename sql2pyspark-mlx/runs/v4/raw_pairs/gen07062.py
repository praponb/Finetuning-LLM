from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull())
    .select("id", "category", "stock_qty")
    .unionByName(
        df_products
        .filter(F.array_contains("tags", "clearance"))
        .select("id", "category", "stock_qty")
    )
    .distinct()
)
