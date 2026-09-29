from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%aci%"))
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    valid
    .groupBy("city")
    .pivot("country", ["JP", "DE"])
    .agg(F.max("rating").alias("value_sum"))
)
