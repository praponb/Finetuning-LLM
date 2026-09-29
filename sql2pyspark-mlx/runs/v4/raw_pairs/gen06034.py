from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("id", "country")
    .subtract(
        df_suppliers
        .filter(F.col("supplier_name").like("%Trade%"))
        .select("id", "country")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("city")
    .orderBy(F.col("city").desc())
)
