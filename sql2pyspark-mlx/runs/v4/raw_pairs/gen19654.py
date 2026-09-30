from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

kept_keys = (
    latest_match
    .select("category", "brand")
    .subtract(
        df_products
        .filter(F.col("product_name").like("%Coff%"))
        .select("category", "brand")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("brand", "id")
    .intersect(
        df_products
        .filter(F.col("product_name").like("%Chair%"))
        .select("brand", "id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.supplier_name"), F.col("p.rating"))
)

result = (
    joined
    .select(
        "id",
        "product_name",
        "brand",
        "launch_date",
        "tags",
        "last_restock",
        "supplier_name",
        "rating",
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("last_restock").desc())
)
