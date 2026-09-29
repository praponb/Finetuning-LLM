from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
    .filter(F.col("unique_categorys") >= 5)
    .orderBy(F.col("total_stock_qty").desc())
)
