from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Water%") & ((F.col("brand") != "Umbrella") | F.col("supplier_id").isNotNull()))
    .select(
        "id",
        "cost",
        F.concat_ws(",", "brand", "product_name").alias("brand_product_name_key"),
    )
)
