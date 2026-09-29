from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(690, 760))
    .groupBy(
        "brand",
        F.when(F.col("category").isin(["Electronics", "Grocery"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(F.count("*").alias("product_count"))
    .filter(F.col("product_count") > 4)
    .orderBy("category_group")
)
