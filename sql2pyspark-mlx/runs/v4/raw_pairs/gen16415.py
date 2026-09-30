from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "noise-cancel headphones")
    .select(
        "*",
        F.upper("product_name").alias("product_name_upper"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

result = (
    filtered
    .select("id", "supplier_id", F.col("price").alias("PRICE_VAL"), "cost")
    .unpivot(["id", "supplier_id"], ["PRICE_VAL", "cost"], "measure", "val")
    .filter(F.col("val").isNotNull())
)
