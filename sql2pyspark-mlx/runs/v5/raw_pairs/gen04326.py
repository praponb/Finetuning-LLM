from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "DE"))
    .select("*", F.trim("country").alias("clean_country"))
)

cleaned = (
    valid
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "country", "city").alias("country_city_key"),
        F.round((F.col("rating") * 1.07), 2).alias("rating_scaled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

cleaned_suppliers = (
    cleaned
    .filter(F.col("city").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Chiang Mai"))))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "clean_country",
        "country_city_key",
        "rating_scaled",
        "city_filled",
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

cleaned_suppliers_2 = (
    cleaned_suppliers
    .filter(F.col("country") != "DE")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "clean_country",
        "country_city_key",
        "rating_scaled",
        "city_filled",
        (F.col("rating") * F.col("rating_scaled")).alias("rating_times_rating_scaled"),
    )
)

latest_match = (
    cleaned_suppliers_2
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.clean_country",
        "s.country_city_key",
        F.col("c.brand").alias("product_brand"),
    )
)

filtered = (
    latest_match
    .filter((F.col("city") == "New York") & F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country_city_key",
        "product_brand",
        F.concat("city", F.lit(" - "), "product_brand").alias("city_product_brand"),
        F.coalesce("product_brand", F.lit("N/A")).alias("product_brand_filled"),
    )
)

stats = (
    filtered
    .rollup("product_brand")
    .agg(
        F.count(F.when(F.col("product_brand") == "Initech", 1)).alias("initech_count"),
        F.count(F.when(F.col("product_brand") == "Acme", 1)).alias("acme_rows"),
        F.grouping("product_brand").alias("grouping_id"),
    )
    .filter(F.col("acme_rows") > 1)
)

result = stats.select("initech_count", "grouping_id")
