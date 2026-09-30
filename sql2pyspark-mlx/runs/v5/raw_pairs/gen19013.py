from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "*",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    base
    .groupBy("city", "segment")
    .pivot("country", ["JP", "UK"])
    .agg(F.sum("age"))
)
