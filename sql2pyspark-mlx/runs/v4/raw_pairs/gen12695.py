from pyspark.sql import functions as F

kept_keys = (
    df_stores
    .select("city")
    .subtract(
        df_stores
        .filter(F.col("region") == "North")
        .select("city")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("region")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
)
