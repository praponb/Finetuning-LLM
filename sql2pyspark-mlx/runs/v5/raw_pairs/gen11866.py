from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", "brand")
    .pivot("category", ["Electronics", "Home"])
    .agg(F.max("price"))
    .select(
        "supplier_id",
        "brand",
        F.col("Electronics").alias("electronics"),
        F.col("Home").alias("home"),
    )
)
