from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("city", "id")
    .intersect(
        df_suppliers
        .filter(F.col("rating") >= 2.9)
        .select("city", "id")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("city").isNotNull())
    .select("*", F.lower("supplier_name").alias("supplier_name_lower"))
)

kept = (
    valid
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") == 269), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") == 388), "left_anti")
)

result = (
    eligible
    .select("id", "supplier_name", "country", "city")
    .orderBy("country")
)
