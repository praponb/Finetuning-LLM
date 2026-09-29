from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([4, 8, 30, 35])), "left_anti")
)
