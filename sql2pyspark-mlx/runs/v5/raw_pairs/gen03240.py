from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Books", "Home"]))
    .groupBy(
        "brand",
        F.when(F.col("price") >= 1330, "high").when(F.col("price") >= 440, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") > 46)
)
