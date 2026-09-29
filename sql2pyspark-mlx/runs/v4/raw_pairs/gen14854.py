from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.price") > 880)
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.launch_date"),
        F.col("p.category"),
    )
)
