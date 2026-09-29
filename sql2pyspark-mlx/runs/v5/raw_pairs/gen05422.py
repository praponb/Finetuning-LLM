from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 3, 6]))
    .select(
        "id",
        "product_name",
        F.concat("product_name", F.lit(", "), "category").alias("product_name_category"),
        F.when(F.col("category").isin(["Books", "Sports"]), "Tier 1").otherwise("Other").alias("category_group"),
    )
    .orderBy("category_group", "id")
    .limit(10)
)
