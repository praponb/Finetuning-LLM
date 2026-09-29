from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("region", "city")
    .intersect(
        df_stores
        .filter(F.col("opened_date").between("2025-05-02", "2025-10-01"))
        .select("region", "city")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 14958, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows_2"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
)
