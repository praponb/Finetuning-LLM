from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 640) & (F.col("stock_qty") == 223))
    .groupBy("brand", "supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.count("*").alias("product_count"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
    .filter(F.col("unique_categorys") < 1)
    .drop("unique_categorys")
    .orderBy(F.col("brand").desc(), F.col("product_count").desc())
)
