from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "laptop pro 14")
    .select("launch_date")
    .union(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 3, 4]))
        .select("sale_date")
    )
)
