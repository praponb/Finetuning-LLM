from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("New York"))) & (F.col("rating") < 2.7))
    .select(
        "*",
        F.when(F.col("country").isin(["TH", "JP"]), "Group A").otherwise("Other").alias("country_group"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("rating") >= 4.0, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.count("*").alias("row_count"),
    )
)
