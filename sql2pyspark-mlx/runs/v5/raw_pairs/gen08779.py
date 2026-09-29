from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
    )
    .orderBy(F.col("price").desc())
)
