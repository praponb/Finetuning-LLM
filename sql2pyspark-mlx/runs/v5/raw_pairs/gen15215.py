from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("brand", "id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("brand", "id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Home|Fashion)$"), "EXTENDED").otherwise("GROUP_A").alias("category_zone"),
    )
)
