from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Stark", "Acme"]) & (F.col("brand") == "Globex"))
    .groupBy(
        F.when(F.col("price") >= 1350, "high").when(F.col("price") >= 580, "mid").otherwise("low").alias("price_bucket"),
        "brand",
        "category",
    )
    .agg(F.avg("stock_qty").alias("avg_stock_qty"))
    .orderBy("price_bucket")
)
