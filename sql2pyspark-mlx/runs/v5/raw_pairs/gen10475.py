from pyspark.sql import functions as F

joined = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country").isin(["DE", "JP", "TH"]))
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("b.supplier_name"),
        F.col("b.city"),
        F.col("b.country"),
    )
)

result = (
    joined
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "launch_date",
        "tags",
        "city",
        "country",
        F.when(F.col("city").rlike("^(Singapore|Bangkok)$"), "GROUP_A").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("country").desc(), "launch_date")
)
