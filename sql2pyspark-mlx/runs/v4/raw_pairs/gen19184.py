from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Books", "Sports"]))
    .groupBy(
        "brand",
        F.when(F.col("price") >= 1010, "high").when(F.col("price") >= 230, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.max("price").alias("max_price"))
    .filter(F.col("max_price") < 1050)
    .orderBy(F.col("max_price").desc())
)
