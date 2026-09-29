from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(357, 384))
    .select("id", "product_name", "launch_date")
    .unionByName(
        df_products
        .filter(F.col("launch_date") > "2025-01-01")
        .select("id", "product_name", "launch_date")
    )
    .orderBy(F.col("product_name").desc())
)
