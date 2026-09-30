from pyspark.sql import functions as F

valid = (
    df_suppliers
    .filter(F.col("rating") < 3.4)
    .withColumn("country_city_key", F.concat_ws("-", "country", "city"))
)

valid_suppliers = (
    valid
    .filter(F.length("supplier_name") > 9)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "country_city_key",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

kept = (
    valid_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.cost") < 220), "left_anti")
)

result = (
    kept
    .select("id", "supplier_name", "city", "city_filled")
    .orderBy(F.col("id").desc())
    .limit(10)
)
