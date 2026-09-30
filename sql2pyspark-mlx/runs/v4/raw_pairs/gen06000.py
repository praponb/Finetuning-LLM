from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.countDistinct("stock_qty").alias("unique_categorys"))
    .filter(F.col("unique_categorys") >= 4)
    .orderBy(F.col("unique_categorys").desc(), "category")
)
