from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["Berlin", "Singapore", "Tokyo"]))
        .select("city", "id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "city", "opened_date", "sqft")
    .orderBy(F.col("sqft").desc_nulls_first())
)
