from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", F.array_contains("tags", "bestseller").alias("is_bestseller"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.supplier_name"), F.col("d.country"), F.col("d.city"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.price",
        "src.cost",
        "src.launch_date",
        "src.supplier_id",
        "src.is_bestseller",
        "src.city",
        F.col("s.rating"),
    )
)

enriched_products = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.supplier_id",
        "s.is_bestseller",
        F.col("b.supplier_name"),
        F.col("b.country"),
        F.col("b.city").alias("supplier_city"),
    )
)

enriched_products_2 = (
    enriched_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select("e.*", F.col("p.city"))
)

result = (
    enriched_products_2
    .select(
        "id",
        "brand",
        "supplier_id",
        "is_bestseller",
        "country",
        "city",
        F.when(F.col("country").rlike("^(DE|TH)$"), "GROUP_B").when(F.col("country").rlike("^(US|SG)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("country_zone"),
    )
)
