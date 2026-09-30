from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.month("sale_date").isin([3, 7, 9])).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("id").desc(), "price")
    .limit(50)
)
