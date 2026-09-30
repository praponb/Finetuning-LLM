from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("id")
    .intersectAll(
        df_stores
        .filter(F.col("city") == "Berlin")
        .select("id")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "store_name", "city", "region", "opened_date")
    .orderBy(F.col("region").desc())
)
