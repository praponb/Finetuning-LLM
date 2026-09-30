from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("country", "id")
    .intersect(
        df_suppliers
        .filter(F.lower("supplier_name").like("%uro%"))
        .select("country", "id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "city", "rating")
    .orderBy(F.col("id").desc(), F.col("city").desc())
)
