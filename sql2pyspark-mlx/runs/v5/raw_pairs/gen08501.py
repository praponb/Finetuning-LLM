from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("ss.units") != 33) & (F.col("p.launch_date") > "2025-06-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("p.cost"),
    )
    .orderBy("sale_date", F.col("store_id").desc())
)
