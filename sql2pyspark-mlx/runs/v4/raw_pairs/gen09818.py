from pyspark.sql import functions as F

enriched = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.rating"))
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "launch_date",
        "supplier_id",
        "rating",
    )
    .orderBy(F.col("id").desc_nulls_last(), "brand")
)
