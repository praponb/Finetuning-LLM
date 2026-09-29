from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 880)
    .select(
        "id",
        "tags",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
        F.substring("brand", 1, 3).alias("brand_prefix"),
    )
    .orderBy("product_name_normalized", "id")
    .limit(20)
)
