from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("city")
    .intersect(
        df_suppliers
        .filter(F.col("rating") < 2.4)
        .select("city")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = verified.select("supplier_name", "country", "city", "rating")
