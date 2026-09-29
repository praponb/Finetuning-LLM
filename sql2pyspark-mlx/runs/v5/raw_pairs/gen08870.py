from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 980, "high").when(F.col("price") >= 470, "mid").otherwise("low").alias("price_bucket"),
        "brand",
    )
    .agg(F.countDistinct("stock_qty").alias("unique_suppliers"))
    .filter(F.col("unique_suppliers") < 1)
    .orderBy("unique_suppliers", "price_bucket", "brand")
    .limit(3)
)
