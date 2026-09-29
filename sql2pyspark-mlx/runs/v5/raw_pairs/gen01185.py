from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("city")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") >= 3.8)
        .select("city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("supplier_name", "city", "rating")
    .orderBy("rating")
)
