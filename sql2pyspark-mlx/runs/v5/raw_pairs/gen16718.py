from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "sale_date",
        "store_id",
        "product_id",
        F.ceil("units").alias("units_whole"),
    )
)
