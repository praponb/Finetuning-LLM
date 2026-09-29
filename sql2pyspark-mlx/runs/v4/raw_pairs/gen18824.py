from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "SG"))
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

result = (
    base
    .select("country", "city")
    .intersect(
        df_suppliers
        .filter(F.col("city").isin(["Tokyo", "Berlin"]))
        .select("country", "city")
    )
)
