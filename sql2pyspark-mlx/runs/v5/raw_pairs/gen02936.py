from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("country_length", F.length("country"))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select("id", "country")
    .intersectAll(
        df_suppliers
        .filter(F.col("country") != "DE")
        .select("id", "country")
    )
)
