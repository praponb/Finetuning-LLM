from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") > 550)
    .select(
        "id",
        "category",
        "stock_qty",
        F.when(F.col("price") >= 1060, "High").when(F.col("price") >= 240, "Medium").otherwise("Low").alias("price_level"),
    )
    .orderBy(F.col("id").desc())
)
