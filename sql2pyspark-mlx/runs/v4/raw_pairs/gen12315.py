from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("store_name").like("Do%"))
        .select("id", "region")
    )
)

reconciled = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = reconciled.select("id", "region", "opened_date", "sqft")
