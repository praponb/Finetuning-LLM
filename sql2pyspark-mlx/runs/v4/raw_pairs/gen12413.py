from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("p.launch_date") > "2023-12-01") & F.col("ss.store_id").isin([1, 5]))
    .select(
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("ss.store_id"),
    )
)
