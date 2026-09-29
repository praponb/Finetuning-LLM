from pyspark.sql import functions as F

matched_keys = (
    df_stores
    .select("region")
    .exceptAll(
        df_stores
        .filter(F.col("region") != "Central")
        .select("region")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.bit_and("sqft").alias("sqft_bit_and"),
    )
    .orderBy(F.col("week_start").desc())
)
