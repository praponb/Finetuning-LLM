from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("brand", "category")
    .subtract(
        df_products
        .filter(F.col("stock_qty") <= 270)
        .select("brand", "category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.when(F.col("brand").rlike("^(Stark|Initech)$"), "CORE").when(F.col("brand").rlike("^(Acme|Globex)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("brand_zone"),
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
