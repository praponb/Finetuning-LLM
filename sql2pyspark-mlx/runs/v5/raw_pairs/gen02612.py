from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([6, 7, 9]) | (F.col("store_id") == 3))
    .select(
        "id",
        "sale_date",
        F.round("units", 0).alias("units_rounded"),
        F.ceil("units").alias("units_whole"),
    )
)
