from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.when(F.col("country").isin(["JP", "UK"]), "Core").otherwise("Other").alias("country_group"),
        F.trim("city").alias("clean_city"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

base = (
    valid
    .filter(~F.col("supplier_name").like("%Trad%"))
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

base_suppliers = (
    base
    .filter(F.col("is_singapore") < 28)
    .select(
        "*",
        F.upper("country").alias("country_upper"),
        F.concat_ws(",", "supplier_name", "city").alias("supplier_name_city_key"),
    )
)

result = (
    base_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "country_group",
        "is_singapore",
        "supplier_name_city_key",
    )
    .orderBy("id")
)
