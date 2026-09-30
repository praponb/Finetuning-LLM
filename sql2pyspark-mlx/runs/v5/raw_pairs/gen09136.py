from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_store_sales
        .filter(F.col("revenue").between(2900, 3500))
        .select("sale_date", F.lit("store_sale").alias("source"))
    )
)
