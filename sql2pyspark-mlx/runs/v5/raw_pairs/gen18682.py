from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("city").eqNullSafe(F.lit("Tokyo")))
    .select(
        "*",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

result = (
    filtered
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(~F.col("email").like("%@startup.io"))
        .select("id", "segment")
    )
)
