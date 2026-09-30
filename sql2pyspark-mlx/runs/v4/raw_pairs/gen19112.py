from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "location")
    .intersectAll(
        df_departments
        .filter(F.col("budget").between(1218000, 3011000))
        .select("id", "location")
    )
)

kept_rows = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "location")
    .orderBy(F.col("location").desc_nulls_first())
)
