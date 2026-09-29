from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(830, 1050) & F.upper("product_name").like("%IRE%"))
    .groupBy("brand")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.countDistinct("cost").alias("unique_categorys"),
    )
    .orderBy(F.col("unique_categorys").desc(), F.col("brand").desc())
)
