from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.year(F.col("ss.sale_date")) == 2024)
    .select(
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("p.category"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)
