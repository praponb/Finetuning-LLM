from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "launch_date")
    .intersect(
        df_products
        .filter(F.col("stock_qty").between(218, 286))
        .select("id", "launch_date")
    )
    .orderBy("launch_date")
)
