from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("brand_product_name", F.concat("brand", F.lit("/"), "product_name"))
)

result = (
    base
    .groupBy("supplier_id")
    .pivot("category", ["Books", "Sports", "Home"])
    .agg(F.sum("price"))
    .select(
        "supplier_id",
        F.col("Books").alias("books"),
        F.col("Sports").alias("sports"),
        F.col("Home").alias("home"),
    )
)
