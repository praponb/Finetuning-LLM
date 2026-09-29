from pyspark.sql import functions as F

kept_keys = (
    df_suppliers
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("G%"))
        .select("id")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "country")
    .orderBy(F.col("country").desc(), "id")
)
