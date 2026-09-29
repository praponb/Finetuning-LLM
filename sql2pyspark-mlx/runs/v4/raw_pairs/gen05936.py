from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("region", "city")
    .intersect(
        df_stores
        .filter(F.col("store_name").like("M%"))
        .select("region", "city")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("store_name", "opened_date", "sqft")
    .orderBy(F.col("sqft").desc(), "opened_date")
)
