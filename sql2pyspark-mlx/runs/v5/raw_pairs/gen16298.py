from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("supplier_id", "brand")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("supplier_id", "brand")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.when(F.col("category").rlike("^(Books|Fashion)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Home)$"), "GROUP_A").otherwise("GROUP_B").alias("category_zone"),
    )
    .orderBy("id", F.col("cost").desc())
)
